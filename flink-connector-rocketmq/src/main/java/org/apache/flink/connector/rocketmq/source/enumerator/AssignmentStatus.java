package org.apache.flink.connector.rocketmq.source.enumerator;

import org.apache.flink.annotation.Internal;

/** status of partition assignment. */
@Internal
public enum AssignmentStatus {

    /** Partitions that have been assigned to readers. */
    ASSIGNED(0),
    /**
     * The partitions that have been discovered during initialization but not assigned to readers
     * yet.
     */
    UNASSIGNED_INITIAL(1);
    private final int statusCode;

    AssignmentStatus(int statusCode) {
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public static AssignmentStatus ofStatusCode(int statusCode) {
        for (AssignmentStatus statusEnum : AssignmentStatus.values()) {
            if (statusEnum.getStatusCode() == statusCode) {
                return statusEnum;
            }
        }
        throw new IllegalArgumentException("statusCode is invalid.");
    }
}
