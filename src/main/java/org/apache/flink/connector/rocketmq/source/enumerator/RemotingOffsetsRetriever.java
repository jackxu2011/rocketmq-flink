package org.apache.flink.connector.rocketmq.source.enumerator;

import org.apache.flink.connector.rocketmq.source.InnerConsumer;
import org.apache.flink.connector.rocketmq.source.enumerator.initializer.OffsetsInitializer;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Collection;
import java.util.Map;

/** The implementation for offsets retriever with a consumer and an admin client. */
public class RemotingOffsetsRetriever
        implements OffsetsInitializer.MessageQueueOffsetsRetriever, AutoCloseable {

    private final InnerConsumer consumer;

    public RemotingOffsetsRetriever(InnerConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void close() throws Exception {
        this.consumer.close();
    }

    @Override
    public Map<MessageQueue, Long> committed(Collection<MessageQueue> partitions) {
        return consumer.committed(partitions);
    }

    @Override
    public Map<MessageQueue, Long> beginOffsets(Collection<MessageQueue> messageQueues) {
        return consumer.beginOffsets(messageQueues);
    }

    @Override
    public Map<MessageQueue, Long> endOffsets(Collection<MessageQueue> messageQueues) {
        return consumer.endOffsets(messageQueues);
    }

    @Override
    public Map<MessageQueue, Long> offsetsForTimes(
            Map<MessageQueue, Long> messageQueueWithTimeMap) {
        return consumer.offsetsForTimes(messageQueueWithTimeMap);
    }
}
