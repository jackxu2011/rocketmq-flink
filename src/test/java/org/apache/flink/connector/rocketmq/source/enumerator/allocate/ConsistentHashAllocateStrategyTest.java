package org.apache.flink.connector.rocketmq.source.enumerator.allocate;

import org.apache.flink.connector.rocketmq.source.split.RocketMQPartitionSplit;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ConsistentHashAllocateStrategyTest {

    private static final String BROKER_NAME = "brokerName";
    private static final String PREFIX_TOPIC = "test-topic-";
    private static final int NUM_SPLITS = 3;
    private static final int[] SPLIT_SIZE = {1000, 2000, 3000};

    @Test
    public void consistentHashAllocateStrategyTest() {
        AllocateStrategy allocateStrategy = new ConsistentHashAllocateStrategy();
        Collection<RocketMQPartitionSplit> mqAll = new ArrayList<>();
        for (int i = 0; i < NUM_SPLITS; i++) {
            mqAll.add(
                    new RocketMQPartitionSplit(
                            PREFIX_TOPIC + (i + 1), BROKER_NAME, i, 0, SPLIT_SIZE[i]));
        }
        int parallelism = 2;
        Map<Integer, Set<RocketMQPartitionSplit>> result =
                allocateStrategy.allocate(mqAll, parallelism);
        for (int i = 0; i < parallelism; i++) {
            Set<RocketMQPartitionSplit> splits = result.getOrDefault(i, new HashSet<>());
            for (RocketMQPartitionSplit split : splits) {
                mqAll.remove(split);
            }
        }
        Assert.assertEquals(0, mqAll.size());
    }
}
