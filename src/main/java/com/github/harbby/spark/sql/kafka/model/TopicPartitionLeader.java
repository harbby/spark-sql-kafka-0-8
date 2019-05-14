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

import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;

import java.io.Serializable;

import static org.spark_project.guava.base.Objects.toStringHelper;

public class TopicPartitionLeader
        implements Serializable
{
    private final TopicAndPartition ktp;
    private final Broker leader;

    public TopicPartitionLeader(TopicAndPartition ktp, Broker leader)
    {
        this.ktp = ktp;
        this.leader = leader;
    }

    public Broker getLeader()
    {
        return leader;
    }

    public TopicAndPartition getKtp()
    {
        return ktp;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("topicAndPartition", ktp)
                .add("broker", leader)
                .toString();
    }
}
