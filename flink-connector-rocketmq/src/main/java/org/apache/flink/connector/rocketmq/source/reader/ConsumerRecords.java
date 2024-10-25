package org.apache.flink.connector.rocketmq.source.reader;

import com.google.common.collect.AbstractIterator;
import org.apache.rocketmq.common.message.MessageQueue;

import javax.annotation.CheckForNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConsumerRecords implements Iterable<MessageView> {

    public static final ConsumerRecords EMPTY = new ConsumerRecords(Collections.emptyMap());

    private final Map<MessageQueue, List<MessageView>> records;

    public ConsumerRecords(Map<MessageQueue, List<MessageView>> records) {
        this.records = records;
    }

    /**
     * Get just the records for the given partition
     *
     * @param partition The partition to get records for
     */
    public List<MessageView> records(MessageQueue partition) {
        List<MessageView> recs = this.records.get(partition);
        if (recs == null) return Collections.emptyList();
        else return Collections.unmodifiableList(recs);
    }

    /** Get just the records for the given topic */
    public Iterable<MessageView> records(String topic) {
        if (topic == null) throw new IllegalArgumentException("Topic must be non-null.");
        List<List<MessageView>> recs = new ArrayList<>();
        for (Map.Entry<MessageQueue, List<MessageView>> entry : records.entrySet()) {
            if (entry.getKey().getTopic().equals(topic)) recs.add(entry.getValue());
        }
        return new ConcatenatedIterable(recs);
    }

    /**
     * Get the partitions which have records contained in this record set.
     *
     * @return the set of partitions with data in this record set (may be empty if no data was
     *     returned)
     */
    public Set<MessageQueue> partitions() {
        return Collections.unmodifiableSet(records.keySet());
    }

    @Override
    public Iterator<MessageView> iterator() {
        return new ConcatenatedIterable(records.values()).iterator();
    }

    /** The number of records for all topics */
    public int count() {
        int count = 0;
        for (List<MessageView> recs : this.records.values()) count += recs.size();
        return count;
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    public static ConsumerRecords empty() {
        return EMPTY;
    }

    private static class ConcatenatedIterable implements Iterable<MessageView> {

        private final Iterable<? extends Iterable<MessageView>> iterables;

        public ConcatenatedIterable(Iterable<? extends Iterable<MessageView>> iterables) {
            this.iterables = iterables;
        }

        @Override
        public Iterator<MessageView> iterator() {
            return new AbstractIterator<MessageView>() {
                final Iterator<? extends Iterable<MessageView>> iters = iterables.iterator();
                Iterator<MessageView> current;

                @CheckForNull
                @Override
                protected MessageView computeNext() {
                    while (current == null || !current.hasNext()) {
                        if (iters.hasNext()) current = iters.next().iterator();
                        else return endOfData();
                    }
                    return current.next();
                }
            };
        }
    }
}
