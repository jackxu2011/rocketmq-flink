/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq.common.config;

import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

/** Config for offsetReset. */
public enum OffsetResetStrategy {
    /** If group offsets is not found,the latest offset would be set to start consumer */
    LATEST,

    /** If group offsets is not found,the earliest offset would be set to start consumer */
    EARLIEST,

    /** If group offsets is not found,the timestamp offset would be set to start consumer */
    TIMESTAMP;

    public ConsumeFromWhere toConsumeFromWhere() {
        switch (this) {
            case LATEST:
                return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
            case EARLIEST:
                return ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;
            case TIMESTAMP:
                return ConsumeFromWhere.CONSUME_FROM_TIMESTAMP;
            default:
                throw new IllegalArgumentException("Unknown offsetResetStrategy: " + this);
        }
    }
}
