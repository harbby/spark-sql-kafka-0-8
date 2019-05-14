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
import com.github.harbby.spark.sql.kafka.model.KafkaSourceOffset;
import com.github.harbby.spark.sql.kafka.model.TopicPartitionLeader;
import kafka.common.TopicAndPartition;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.ContinuousReadSupport;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Map$;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static com.github.harbby.spark.sql.kafka.util.KafkaUtil.getBrokers;
import static com.github.harbby.spark.sql.kafka.util.KafkaUtil.getFromOffset;
import static com.github.harbby.spark.sql.kafka.util.PropertiesUtil.getInt;
import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class KafkaDataSource08
        implements DataSourceV2, ContinuousReadSupport, DataSourceRegister
{
    private static final Logger logger = LoggerFactory.getLogger(KafkaDataSource08.class);
    private static final String dummyClientId = "sylph-spark-kafka-consumer-partition-lookup";

    private static final StructType schema = new StructType(new StructField[] {
            new StructField("_key", BinaryType, true, Metadata.empty()),
            new StructField("_message", BinaryType, true, Metadata.empty()),
            new StructField("_topic", StringType, false, Metadata.empty()),
            new StructField("_partition", IntegerType, false, Metadata.empty()),
            new StructField("_offset", LongType, false, Metadata.empty())
    });

    @Override
    public String shortName()
    {
        return "kafka08";
    }

    @Override
    public ContinuousReader createContinuousReader(Optional<StructType> schema, String checkpointLocation, DataSourceOptions options)
    {
        Properties properties = new Properties();
        properties.putAll(options.asMap());
        String[] topics = requireNonNull(properties.getProperty("topics"), "kafka_topic not setting").split(",");
        String groupId = requireNonNull(properties.getProperty("group.id"), "group.id not setting");

        scala.collection.immutable.Map<String, String> map = (scala.collection.immutable.Map<String, String>) Map$.MODULE$.apply(JavaConverters.mapAsScalaMapConverter(options.asMap()).asScala().toSeq());
        final KafkaCluster kafkaCluster = new KafkaCluster(map);

        int commitInterval = getInt(properties, ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 90000);
        return new KafkaContinuousReader08(options, properties, topics, groupId, kafkaCluster);
    }

    public static class KafkaContinuousReader08
            implements ContinuousReader
    {
        private final DataSourceOptions options;
        private final Properties properties;
        private final Set<String> topics;
        private final KafkaCluster kafkaCluster;
        private final String groupId;

        private Offset lastCommit;
        private Map<TopicAndPartition, Long> fromOffsets;

        public KafkaContinuousReader08(
                DataSourceOptions options,
                Properties properties,
                String[] topics,
                String groupId,
                KafkaCluster kafkaCluster)
        {
            this.options = options;
            this.properties = properties;
            this.kafkaCluster = kafkaCluster;
            this.topics = Arrays.stream(topics).collect(Collectors.toSet());
            this.groupId = groupId;
        }

        private List<TopicPartitionLeader> getKafkaBrokers()
        {
            int soTimeout = getInt(properties, "socket.timeout.ms", 30000);
            int bufferSize = getInt(properties, "socket.receive.buffer.bytes", 65536);

            String[] brokers = properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).split(",");
            URL url = getHostnamePortUrl(brokers[new Random().nextInt(brokers.length)]);

            SimpleConsumer consumer = new SimpleConsumer(url.getHost(), url.getPort(), soTimeout, bufferSize, dummyClientId);
            try {
                return getBrokers(consumer, new ArrayList<>(topics), brokers);
            }
            finally {
                consumer.close();
            }
        }

        @Override
        public Offset mergeOffsets(PartitionOffset[] offsets)
        {
            Map<TopicAndPartition, Long> partitionToOffsets = Arrays
                    .stream(offsets)
                    .map(x -> (KafkaPartitionOffset) x)
                    .collect(Collectors.toMap(k -> k.getTopicPartition(), v -> v.getOffset()));

            return new KafkaSourceOffset(partitionToOffsets);
        }

        @Override
        public Offset deserializeOffset(String json)
        {
            return KafkaSourceOffset.format(json);
        }

        @Override
        public void setStartOffset(Optional<Offset> start)
        {
            if (start.isPresent() && start.get() instanceof KafkaSourceOffset) {
                this.fromOffsets = KafkaSourceOffset.getPartitionOffsets(start.get());
                logger.warn("setting StartOffset {}, Will use checkpoint get startOffset", start);
            }
            else {
                this.fromOffsets = getFromOffset(kafkaCluster, topics, groupId);
                logger.info("setting StartOffset {}, Will use kafka cluster get startOffset", fromOffsets);
            }
        }

        @Override
        public Offset getStartOffset()
        {
            return new KafkaSourceOffset(fromOffsets);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void commit(Offset end)
        {
            //---this.mergeOffsets()
            //通过merge 而来这里放心提交,且此处位于driver
            if (!end.equals(lastCommit)) {
                Map<TopicAndPartition, Long> offsets = ((KafkaSourceOffset) end).getPartitionToOffsets();

                logger.info("committing offset to kafka, {}", offsets);
                Seq<Tuple2<TopicAndPartition, Long>> fromOffsetsAsJava = JavaConverters.mapAsScalaMapConverter(offsets).asScala().toSeq();
                kafkaCluster.setConsumerOffsets(groupId, (scala.collection.immutable.Map<TopicAndPartition, Object>) Map$.MODULE$.apply(fromOffsetsAsJava));
                this.lastCommit = end;
            }
        }

        @Override
        public void stop()
        {
        }

        @Override
        public StructType readSchema()
        {
            return schema;
        }

        @Override
        public List<InputPartition<InternalRow>> planInputPartitions()
        {
            logger.info("getting kafka topic {}, partition info", topics);
            List<TopicPartitionLeader> topicPartitionLeaders = getKafkaBrokers();
            List<InputPartition<InternalRow>> partitions = new ArrayList<>(topicPartitionLeaders.size());

            for (TopicPartitionLeader topicPartitionLeader : topicPartitionLeaders) {
                Long formOffset = fromOffsets.get(topicPartitionLeader.getKtp());
                requireNonNull(formOffset, topicPartitionLeader.getKtp() + " not found formOffset");
                InputPartition<InternalRow> inputPartition = new KafkaInputPartition08(
                        topicPartitionLeader.getKtp(),
                        formOffset,
                        properties,
                        topicPartitionLeader.getLeader()
                );
                partitions.add(inputPartition);
            }

            logger.info("creating partitions {}", partitions);
            return partitions;
        }
    }

    private static URL getHostnamePortUrl(String hostPort)
    {
        try {
            URL u = new URL("http://" + hostPort);
            if (u.getHost() == null) {
                throw new IllegalArgumentException("The given host:port ('" + hostPort + "') doesn't contain a valid host");
            }
            if (u.getPort() == -1) {
                throw new IllegalArgumentException("The given host:port ('" + hostPort + "') doesn't contain a valid port");
            }
            return u;
        }
        catch (MalformedURLException e) {
            throw new IllegalArgumentException("The given host:port ('" + hostPort + "') is invalid", e);
        }
    }
}
