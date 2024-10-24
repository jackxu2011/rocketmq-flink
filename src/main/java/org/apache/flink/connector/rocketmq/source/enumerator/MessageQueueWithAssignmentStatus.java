package org.apache.flink.connector.rocketmq.source.enumerator;

import org.apache.rocketmq.common.message.MessageQueue;

/** RecketMQ MessageQueue with assign status. */
public class MessageQueueWithAssignmentStatus {
    private final MessageQueue messageQueue;
    private final AssignmentStatus assignmentStatus;

    public MessageQueueWithAssignmentStatus(
            MessageQueue messageQueue, AssignmentStatus assignmentStatus) {
        this.messageQueue = messageQueue;
        this.assignmentStatus = assignmentStatus;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public AssignmentStatus getAssignmentStatus() {
        return assignmentStatus;
    }
}
