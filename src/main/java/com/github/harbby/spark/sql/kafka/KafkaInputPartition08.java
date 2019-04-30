/*
 * Copyright (C) 2018 The Sylph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.harbby.spark.sql.kafka;

import com.github.harbby.spark.sql.kafka.model.KafkaPartitionOffset;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousInputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Iterator;
import java.util.Properties;

import static com.github.harbby.spark.sql.kafka.util.PropertiesUtil.getInt;

/**
 * demo: org.apache.flink.streaming.connectors.kafka.internals.SimpleConsumerThread
 */
public class KafkaInputPartition08
        implements InputPartition<InternalRow>
{
    private static final Logger logger = LoggerFactory.getLogger(KafkaInputPartition08.class);

    private final TopicAndPartition topicPartition;
    private final long startOffset;
    private final Properties kafkaParams;
    private final Broker broker;

    public KafkaInputPartition08(
            TopicAndPartition topicPartition,
            long startOffset,
            Properties kafkaParams,
            Broker broker
    )
    {
        this.topicPartition = topicPartition;
        this.startOffset = startOffset;
        this.kafkaParams = kafkaParams;
        this.broker = broker;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader()
    {
        return new KafkaInputPartitionReader08(topicPartition, startOffset, kafkaParams, broker);
    }

    /**
     * local 本地化
     */
    @Override
    public String[] preferredLocations()
    {
        return new String[] {broker.host()};
    }

    private static class KafkaInputPartitionReader08
            implements ContinuousInputPartitionReader<InternalRow>
    {
        private final TopicAndPartition topicPartition;
        private final Broker broker;

        private final int soTimeout;
        private final int minBytes;
        private final int maxWait;
        private final int fetchSize;
        private final int bufferSize;
        private final int recreateConsumerLimit;
        private final String clientId;

        private SimpleConsumer consumer;
        private long currentOffset;
        private int recreateConsumerNum = 0;

        public KafkaInputPartitionReader08(
                TopicAndPartition topicPartition,
                long startOffset,
                Properties kafkaParams,
                Broker broker
        )
        {
            this.topicPartition = topicPartition;
            this.broker = broker;
            this.currentOffset = startOffset;

            // these are the actual configuration values of Kafka + their original default values.
            this.soTimeout = getInt(kafkaParams, "socket.timeout.ms", 30000);
            this.minBytes = getInt(kafkaParams, "fetch.min.bytes", 1);
            this.maxWait = getInt(kafkaParams, "fetch.wait.max.ms", 500);
            this.fetchSize = getInt(kafkaParams, "fetch.message.max.bytes", 1048576);   //1M
            this.bufferSize = getInt(kafkaParams, "socket.receive.buffer.bytes", 65536); //64k
            this.recreateConsumerLimit = getInt(kafkaParams, "sylph.spark.simple-consumer-reconnectLimit", 3);
            String groupId = kafkaParams.getProperty("group.id", "sylph-spark-kafka-consumer-legacy-" + broker.id());
            this.clientId = kafkaParams.getProperty("client.id", groupId);

            // create the Kafka consumer that we actually use for fetching
            this.consumer = new SimpleConsumer(broker.host(), broker.port(), soTimeout, bufferSize, clientId);
        }

        private Iterator<MessageAndOffset> fetchIterator;

        /**
         * see: org.apache.flink.streaming.connectors.kafka.internals.SimpleConsumerThread
         */
        private Iterator<MessageAndOffset> fetch()
                throws IOException
        {
            FetchRequestBuilder frb = new FetchRequestBuilder();
            frb.clientId(clientId);
            frb.maxWait(maxWait);
            frb.minBytes(minBytes);
            frb.addFetch(
                    topicPartition.topic(),
                    topicPartition.partition(),
                    currentOffset + 1, // request the next record
                    fetchSize);

            FetchRequest fetchRequest = frb.build();

            final FetchResponse fetchResponse;
            try {
                fetchResponse = consumer.fetch(fetchRequest);
            }
            catch (Exception e) {
                if (e instanceof ClosedChannelException) {
                    logger.error("Fetch failed!", e);

                    try {
                        consumer.close();
                    }
                    catch (Exception e1) {
                        logger.error("consumer close failed", e1);
                    }

                    if (recreateConsumerNum++ > recreateConsumerLimit) {
                        throw new IOException("Consumer recreate Number > " + recreateConsumerLimit, e);
                    }

                    try {
                        Thread.sleep(100);
                    }
                    catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }

                    this.consumer = new SimpleConsumer(broker.host(), broker.port(), soTimeout, bufferSize, clientId);
                    return fetch();
                }
                throw e;
            }
            if (fetchResponse == null) {
                throw new IOException("Fetch from Kafka failed (request returned null)");
            }

            recreateConsumerNum = 0;

            final ByteBufferMessageSet messageSet = fetchResponse.messageSet(
                    topicPartition.topic(), topicPartition.partition());

            return messageSet.iterator();
        }

        @Override
        public boolean next()
                throws IOException
        {
            while (fetchIterator == null || !fetchIterator.hasNext()) {
                if (TaskContext.get().isInterrupted() || TaskContext.get().isCompleted()) {
                    return false;
                }

                this.fetchIterator = fetch();
            }
            return true;
        }

        @Override
        public InternalRow get()
        {
            MessageAndOffset msg = fetchIterator.next();
            final ByteBuffer payload = msg.message().payload();
            final long offset = msg.offset();

            // If the message value is null, this represents a delete command for the message key.
            // Log this and pass it on to the client who might want to also receive delete messages.
            byte[] valueBytes;
            if (payload == null) {
                valueBytes = null;
            }
            else {
                valueBytes = new byte[payload.remaining()];
                payload.get(valueBytes);
            }

            // put key into byte array
            byte[] keyBytes = null;
            int keySize = msg.message().keySize();

            if (keySize >= 0) { // message().hasKey() is doing the same. We save one int deserialization
                ByteBuffer keyPayload = msg.message().key();
                keyBytes = new byte[keySize];
                keyPayload.get(keyBytes);
            }

            this.currentOffset = offset;
            return new GenericInternalRow(new Object[] {
                    keyBytes,
                    valueBytes,
                    UTF8String.fromString(topicPartition.topic()),
                    topicPartition.partition(),
                    offset});
        }

        @Override
        public void close()
                throws IOException
        {
            if (consumer != null) {
                consumer.close();
            }
        }

        @Override
        public PartitionOffset getOffset()
        {
            return new KafkaPartitionOffset(topicPartition, currentOffset);
        }
    }
}
