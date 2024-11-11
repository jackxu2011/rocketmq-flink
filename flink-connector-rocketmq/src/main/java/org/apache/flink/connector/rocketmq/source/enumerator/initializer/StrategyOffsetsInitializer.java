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

package org.apache.flink.connector.rocketmq.source.enumerator.initializer;

import org.apache.flink.connector.rocketmq.common.config.OffsetResetStrategy;

import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Collection;
import java.util.Map;

public class StrategyOffsetsInitializer implements OffsetsInitializer {

    private final ConsumeFromWhere fromWhere;
    private final OffsetResetStrategy offsetResetStrategy;

    StrategyOffsetsInitializer(
            ConsumeFromWhere fromWhere, OffsetResetStrategy offsetResetStrategy) {
        this.fromWhere = fromWhere;
        this.offsetResetStrategy = offsetResetStrategy;
    }

    @Override
    public Map<MessageQueue, Long> getMessageQueueOffsets(
            Collection<MessageQueue> messageQueues, MessageQueueOffsetsRetriever offsetsRetriever) {

        Map<MessageQueue, Long> initialOffsets;
        switch (fromWhere) {
            case CONSUME_FROM_LAST_OFFSET:
                initialOffsets = offsetsRetriever.endOffsets(messageQueues);
                break;
            case CONSUME_FROM_FIRST_OFFSET:
                initialOffsets = offsetsRetriever.beginOffsets(messageQueues);
                break;
            case CONSUME_FROM_TIMESTAMP:
                initialOffsets = offsetsRetriever.committed(messageQueues);
                break;
            default:
                throw new IllegalStateException("Unknown consume from where: " + fromWhere);
        }
        return initialOffsets;
    }

    @Override
    public OffsetResetStrategy getAutoOffsetResetStrategy() {
        return offsetResetStrategy;
    }

    @Override
    public ConsumeFromWhere getConsumeFromWhere() {
        switch (fromWhere) {
            case CONSUME_FROM_LAST_OFFSET:
            case CONSUME_FROM_FIRST_OFFSET:
                return fromWhere;
            default:
                return ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;
        }
    }
}
