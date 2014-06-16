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

    private ListMultimap<String, byte[]> topicToData;
    private Map<String, Map<String, Integer>> consumerToTopics;
    private List<Pair<String, byte[]>> expirationQueue;
    private int maxMessages;

    public SimpleMessageQueue() {
        // If no max is given, then try the system property, with 10000 as the
        // default.
        this(Integer.parseInt(System.getProperty(MAX_MESSAGE_PROP, "10000")));
    }

    public SimpleMessageQueue(int maxMessages) {
        this.maxMessages = maxMessages;
        this.topicToData = ArrayListMultimap.create();
        this.consumerToTopics = Maps.newHashMap();
        this.expirationQueue = Lists.newLinkedList();
        log.info("Message queue initialized with max size of {}", this.maxMessages);
    }

    public synchronized void enqueue(String topic, byte[] data) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic), "topic cannot be null or empty");
        Preconditions.checkNotNull(data, "data cannot be null");

        if (expirationQueue.size() == maxMessages) {
            Pair<String, byte[]> pair = expirationQueue.remove(expirationQueue.size() - 1);
            topicToData.remove(pair.getValue0(), pair.getValue1());
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
        topicToData.put(topic, data);
        Pair<String, byte[]> pair = new Pair<String, byte[]>(topic, data);
        expirationQueue.add(0, pair);
    }

    public synchronized Optional<byte[]> dequeue(String consumerId, String topic) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(consumerId), "consumerId cannot be null or empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic), "topic cannot be null or empty");
        Optional<byte[]> result = Optional.absent();
        Map<String, Integer> topicToIndex = new HashMap<String, Integer>();
        int nextIndex = 0;

        if (consumerToTopics.containsKey(consumerId)) {
            topicToIndex = consumerToTopics.get(consumerId);
            Integer nextIndexTest = topicToIndex.get(topic);
            if (nextIndexTest != null) {
                nextIndex = nextIndexTest;
            }
        }

        if (topicToData.containsKey(topic)) {
            List<byte[]> dataQueue = topicToData.get(topic);
            if (dataQueue.size() > nextIndex) {
                result = Optional.of(dataQueue.get(nextIndex++));
            }
        }
        topicToIndex.put(topic, nextIndex);
        consumerToTopics.put(consumerId, topicToIndex);

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
