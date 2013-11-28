package com.simplemq.core;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
    private List<IndexedPair> expirationQueue;
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
        Preconditions.checkNotNull(topic, "topic cannot be null");
        Preconditions.checkNotNull(data, "data cannot be null");

        if (expirationQueue.size() == maxMessages) {
            IndexedPair pair = expirationQueue.remove(expirationQueue.size() - 1);
            topicToData.remove(pair.key, pair.value);
        }
        topicToData.put(topic, data);
        IndexedPair pair = new IndexedPair();
        pair.key = topic;
        pair.value = data;
        expirationQueue.add(0, pair);
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

    public synchronized void resetTopic(String consumerId, String topic) {
        if (consumerToTopics.containsKey(consumerId)) {
            Map<String, Integer> topicToIndex = consumerToTopics.get(consumerId);
            if (topicToIndex.containsKey(topic)) {
                topicToIndex.remove(topic);
            }
        }
    }

    private static class IndexedPair {
        public String key;
        public byte[] value;
    }
}
