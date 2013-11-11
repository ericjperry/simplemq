package com.simplemq.client;

import com.google.common.base.Optional;
import com.simplemq.thrift.SimpleMQ;
import com.simplemq.thrift.SimpleMQMessage;
import com.simplemq.thrift.SimpleMQPollResult;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SimpleMQClient implements Closeable {
    private static Logger log = LoggerFactory.getLogger(SimpleMQClient.class);
    private TTransport transport;
    private SimpleMQ.Client client;
    private int timeout;
    private String consumerId;

    public SimpleMQClient(String hostname, int port, String consumerId, int timeout) throws TTransportException {
        this.transport = new TSocket(hostname, port);
        this.transport.open();

        TProtocol protocol = new TBinaryProtocol(this.transport);
        this.client = new SimpleMQ.Client(protocol);
        this.timeout = timeout;
        this.consumerId = consumerId;
    }

    public void send(String topic, byte[] message) {
        try {
            client.send(new SimpleMQMessage(topic, ByteBuffer.wrap(message)));
        } catch (TException e) {
            log.error("Could not send message", e);
        }
    }

    public Optional<byte[]> poll(String topic) {
        Optional<byte[]> result = Optional.absent();
        try {
            SimpleMQPollResult pollResult = client.poll(consumerId, topic, timeout);
            if (pollResult.getMessage() != null) {
                result = Optional.of(pollResult.getMessage().getData());
            }
        } catch (TException e) {
            log.error(String.format("Could not poll topic '%s'", topic), e);
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        transport.close();
    }
}
