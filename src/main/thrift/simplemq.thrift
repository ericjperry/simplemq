namespace java com.simplemq.thrift

struct SimpleMQMessage {
    1: required string topic;
    2: required binary data;
}

struct SimpleMQPollResult {
    1: optional SimpleMQMessage message;
}

service SimpleMQ {
    void send(1: SimpleMQMessage message),
    void unsubscribeFromTopic(1: string consumerId, 2: string topic),
    SimpleMQPollResult poll(1: string consumerId, 2: string topic, 3: i32 timeout);
}