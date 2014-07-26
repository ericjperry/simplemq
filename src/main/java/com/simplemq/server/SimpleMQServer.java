package com.simplemq.server;

import com.google.common.base.Optional;
import com.simplemq.core.SimpleMessageQueue;
import com.simplemq.thrift.SimpleMQ;
import com.simplemq.thrift.SimpleMQMessage;
import com.simplemq.thrift.SimpleMQPollResult;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class SimpleMQServer {
    private static Logger logger = LoggerFactory.getLogger(SimpleMQServer.class);
    private TServer server;

    protected static class LocalMQHandler implements SimpleMQ.Iface {
        private SimpleMessageQueue mq;

        public LocalMQHandler() {
            mq = new SimpleMessageQueue();
        }

        @Override
        public void send(SimpleMQMessage message) throws TException {
            mq.enqueue(message.getTopic(), message.getData());
        }

        @Override
        public void unsubscribeFromTopic(String consumerId, String topic) throws TException {
            mq.resetTopic(consumerId, topic);
        }

        @Override
        public SimpleMQPollResult poll(String consumerId, String topic, int timeout) throws TException {
            long endTimeMillis = System.currentTimeMillis() + timeout;
            Optional<byte[]> dequeueResult = Optional.absent();
            SimpleMQPollResult pollResult = new SimpleMQPollResult();
            while (System.currentTimeMillis() < endTimeMillis && !dequeueResult.isPresent()) {
                dequeueResult = mq.dequeue(consumerId, topic);
            }

            if (dequeueResult.isPresent()) {
                pollResult.setMessage(new SimpleMQMessage(topic, ByteBuffer.wrap(dequeueResult.get())));
            }
            return pollResult;
        }

    }

    public SimpleMQServer(int port) {
        LocalMQHandler handler = new LocalMQHandler();
        SimpleMQ.Processor processor = new SimpleMQ.Processor(handler);

        TServerTransport serverTransport;
        try {
            serverTransport = new TServerSocket(port);
        } catch (TTransportException e) {
            throw new RuntimeException("Could not create server socket", e);
        }
        server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
    }

    /**
     * This method does not return and relies on thread management by the consumer of this class.
     */
    public void start() {
        server.serve();
    }

    public static void main(String[] args) throws TTransportException {
        if (args.length == 1) {
            System.out.println("Starting the LocalMQ Server...");

            final SimpleMQServer mqServer;
            try {
                mqServer = new SimpleMQServer(Integer.parseInt(args[0]));
            } catch (NumberFormatException e) {
                throw new RuntimeException("Please provide an integer as a command line parameter to use as the server port", e);
            }
            mqServer.start();
        } else {
            System.out.println("Please provide a port for the LocalMQ Server.");
            System.out.println("    " + SimpleMQServer.class.getSimpleName() + " <port>");
        }
    }
}
