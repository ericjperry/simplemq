package com.simplemq.core;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.*;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleMessageQueue {
    private static final Logger log = LoggerFactory.getLogger(SimpleMessageQueue.class);
    public static final String MAX_MESSAGE_PROP = "com.simplemq.core.maxMessages";
    public static final String RETAIN_MESSAGE_PROP = "com.simplemq.core.retain";

    private ListMultimap<String, byte[]> topicToData;
    private Map<String, Map<String, Integer>> consumerToTopics;
    private List<String> expirationQueue;
    private long maxMessages;
    private boolean retainMessages;

    public SimpleMessageQueue() {
        // If no max is given, then try the system property, with 10000 as the
        // default. Queue defaults to retain messages.
        this(Long.parseLong(System.getProperty(MAX_MESSAGE_PROP, "10000")),
                Boolean.parseBoolean(System.getProperty((RETAIN_MESSAGE_PROP), "true")));
    }

    public SimpleMessageQueue(long maxMessages, boolean retainMessages) {
        this.maxMessages = maxMessages;
        this.topicToData = LinkedListMultimap.create();
        this.consumerToTopics = Maps.newHashMap();
        this.expirationQueue = Lists.newLinkedList();
        this.retainMessages = retainMessages;
        if (this.retainMessages) {
            log.info("Message queue initialized with max size of {}", this.maxMessages);
        }
    }

    public synchronized void enqueue(String topic, byte[] data) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic), "topic cannot be null or empty");
        Preconditions.checkNotNull(data, "data cannot be null");

        if (retainMessages) {
            if (expirationQueue.size() == maxMessages) {
                String toRemove = expirationQueue.remove(expirationQueue.size() - 1);
                log.debug("Removing message from topic {}", toRemove);
                topicToData.get(toRemove).remove(0);
                for (String consumer : consumerToTopics.keySet()) {
                    Map<String, Integer> topicToIndices = consumerToTopics.get(consumer);
                    if (topicToIndices.containsKey(topic)) {
                        int index = topicToIndices.get(topic);
                        if (index > 0) {
                            topicToIndices.put(topic, index - 1);
                        }
                    }
                }
            }
            expirationQueue.add(0, topic);
        }
        topicToData.put(topic, data);
    }

    public synchronized Optional<byte[]> dequeue(String consumerId, String topic) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(consumerId), "consumerId cannot be null or empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic), "topic cannot be null or empty");
        Optional<byte[]> result = Optional.absent();

        if (topicToData.containsKey(topic)) {
            List<byte[]> dataQueue = topicToData.get(topic);

            // If messages are being retained, calculate the index at which we are consuming and
            // leave the consumed message in the queue for any other consumer IDs
            if (retainMessages) {
                Map<String, Integer> topicToIndex = new HashMap<String, Integer>();
                int nextIndex = 0;

                if (consumerToTopics.containsKey(consumerId)) {
                    topicToIndex = consumerToTopics.get(consumerId);
                    Integer nextIndexTest = topicToIndex.get(topic);
                    if (nextIndexTest != null) {
                        nextIndex = nextIndexTest;
                    }
                }

                if (dataQueue.size() > nextIndex) {
                    result = Optional.of(dataQueue.get(nextIndex++));
                }
                topicToIndex.put(topic, nextIndex);
                consumerToTopics.put(consumerId, topicToIndex);
            } else {
                // If we aren't retaining messages, simply remove the top of the queue
                if (dataQueue.size() > 0) {
                    result = Optional.of(dataQueue.remove(0));
                }
            }
        }
        return result;
    }

    public synchronized void resetTopic(String consumerId, String topic) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(consumerId), "consumerId cannot be null or empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic), "topic cannot be null or empty");
        if (consumerToTopics.containsKey(consumerId)) {
            Map<String, Integer> topicToIndex = consumerToTopics.get(consumerId);
            if (topicToIndex.containsKey(topic)) {
                topicToIndex.remove(topic);
            }
        }
    }
}
