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
package com.github.harbby.spark.sql.kafka.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;
import kafka.common.TopicAndPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class KafkaSourceOffset
        extends Offset
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Map<TopicAndPartition, Long> partitionToOffsets;

    public KafkaSourceOffset(Map<TopicAndPartition, Long> partitionToOffsets)
    {
        this.partitionToOffsets = partitionToOffsets;
    }

    public Map<TopicAndPartition, Long> getPartitionToOffsets()
    {
        return partitionToOffsets;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        KafkaSourceOffset o = (KafkaSourceOffset) obj;
        return Objects.equals(partitionToOffsets, o.partitionToOffsets);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionToOffsets);
    }

    public static Map<TopicAndPartition, Long> getPartitionOffsets(Offset offset)
    {
        if (offset instanceof KafkaSourceOffset) {
            return ((KafkaSourceOffset) offset).getPartitionToOffsets();
        }
        else {
            throw new IllegalArgumentException("Invalid conversion from offset of " + offset.getClass() + " to KafkaSourceOffset");
        }
    }

    public static KafkaSourceOffset format(String json)
    {
        MapType mapType = MapType.construct(HashMap.class, SimpleType.construct(Integer.class), SimpleType.construct(Long.class));

        try {
            Map<String, Map<Integer, Long>> result = MAPPER.readValue(json, MapType.construct(HashMap.class, SimpleType.construct(String.class), mapType));

            Map<TopicAndPartition, Long> partitionToOffsets = result.entrySet()
                    .stream()
                    .flatMap(x -> x.getValue().entrySet().stream().map(y -> {
                        return new KafkaPartitionOffset(TopicAndPartition.apply(x.getKey(), y.getKey()), y.getValue());
                    }))
                    .collect(Collectors.toMap(k -> k.getTopicPartition(), v -> v.getOffset()));

            return new KafkaSourceOffset(partitionToOffsets);
        }
        catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public String json()
    {
        Map<String, Map<Integer, Long>> result = new HashMap<>();
        partitionToOffsets.forEach((topicAndPartition, offset) -> {
            String topic = topicAndPartition.topic();
            result.computeIfAbsent(topic, (key) -> new HashMap<>())
                    .put(topicAndPartition.partition(), offset);
        });

        try {
            return MAPPER.writeValueAsString(result);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
