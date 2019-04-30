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

import kafka.common.TopicAndPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset;

import java.io.Serializable;

public class KafkaPartitionOffset
        implements PartitionOffset, Serializable
{
    private final TopicAndPartition topicPartition;
    private final long offset;

    public KafkaPartitionOffset(TopicAndPartition topicPartition, long offset)
    {
        this.topicPartition = topicPartition;
        this.offset = offset;
    }

    public long getOffset()
    {
        return offset;
    }

    public TopicAndPartition getTopicPartition()
    {
        return topicPartition;
    }
}
