package com.github.harbby.spark.sql.kafka.util;

import com.github.harbby.spark.sql.kafka.model.TopicPartitionLeader;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils$;
import scala.collection.JavaConverters;
import scala.collection.mutable.ArrayBuffer;
import scala.util.Either;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaUtil
{
    private KafkaUtil() {}

    public static Map<TopicAndPartition, Long> getFromOffset(KafkaCluster kafkaCluster, String topics, String groupId)
    {
        Set<String> topicSets = Arrays.stream(topics.split(",")).collect(Collectors.toSet());
        return getFromOffset(kafkaCluster, topicSets, groupId);
    }

    public static Map<TopicAndPartition, Long> getFromOffset(KafkaCluster kafkaCluster, Set<String> topics, String groupId)
    {
        scala.collection.immutable.Set<String> scalaTopicSets = JavaConverters.asScalaSetConverter(topics).asScala().toSet();

        Either<ArrayBuffer<Throwable>, scala.collection.immutable.Map<TopicAndPartition, Object>> groupOffsets = kafkaCluster.getConsumerOffsets(
                groupId,
                kafkaCluster.getPartitions(scalaTopicSets).right().get());

        scala.collection.immutable.Map<TopicAndPartition, Object> fromOffsets;
        if (groupOffsets.isRight()) {
            fromOffsets = groupOffsets.right().get();
        }
        else {
            fromOffsets = KafkaUtils$.MODULE$.getFromOffsets(kafkaCluster, kafkaCluster.kafkaParams(), scalaTopicSets);
        }
        return JavaConverters.mapAsJavaMapConverter(fromOffsets).asJava()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, v -> (long) v.getValue()));
    }


    public static List<TopicPartitionLeader> getBrokers(SimpleConsumer consumer, List<String> topics, String[] brokers)
    {
        List<TopicPartitionLeader> partitionLeaders = new ArrayList<>();
        for (TopicMetadata item : consumer.send(new TopicMetadataRequest(topics)).topicsMetadata()) {
            if (item.errorCode() != ErrorMapping.NoError()) {
                throw new IllegalArgumentException("Error while getting metadata from broker " + brokers +
                        " to find partitions for " + topics.toString() + ". Error: {}" +
                        ErrorMapping.exceptionFor(item.errorCode()).getMessage());
            }

            if (!topics.contains(item.topic())) {
                throw new IllegalArgumentException("Received metadata from topic " + item.topic()
                        + " even though it was not requested. Skipping ...");
            }

            for (PartitionMetadata part : item.partitionsMetadata()) {
                Broker leader = new Broker(part.leader().id(), part.leader().host(), part.leader().port());
                TopicAndPartition ktp = new TopicAndPartition(item.topic(), part.partitionId());
                TopicPartitionLeader pInfo = new TopicPartitionLeader(ktp, leader);
                partitionLeaders.add(pInfo);
            }
        }

        return partitionLeaders;
    }
}
