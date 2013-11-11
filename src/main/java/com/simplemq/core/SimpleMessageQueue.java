package com.simplemq.core;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleMessageQueue {
    private ListMultimap<String, byte[]> topicToData;
    private Map<String, Map<String, Integer>> consumerToTopics;

    public SimpleMessageQueue() {
        topicToData = ArrayListMultimap.create();
        consumerToTopics = Maps.newHashMap();
    }

    public synchronized void enqueue(String topic, byte[] data) {
        Preconditions.checkNotNull(topic, "topic cannot be null");
        Preconditions.checkNotNull(data, "data cannot be null");

        topicToData.put(topic, data);
    }

    public synchronized Optional<byte[]> dequeue(String consumerId, String topic) {
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
}
