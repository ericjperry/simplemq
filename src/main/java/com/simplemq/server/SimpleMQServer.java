package com.simplemq.server;

import com.google.common.base.Optional;
import com.simplemq.core.SimpleMessageQueue;
import com.simplemq.thrift.SimpleMQ;
import com.simplemq.thrift.SimpleMQMessage;
import com.simplemq.thrift.SimpleMQPollResult;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;

public class SimpleMQServer {

    protected static class LocalMQHandler implements SimpleMQ.Iface {
        private SimpleMessageQueue mq;

        public LocalMQHandler() {
            mq = new SimpleMessageQueue();
        }

        public void send(SimpleMQMessage message) {
            mq.enqueue(message.getTopic(), message.getData());
        }

        public SimpleMQPollResult poll(String consumerId, String topic, int timeout) {
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

    public static void main(String[] args) throws TTransportException {
        if (args.length == 1) {
            LocalMQHandler handler = new LocalMQHandler();
            SimpleMQ.Processor processor = new SimpleMQ.Processor(handler);

            TServerTransport serverTransport = new TServerSocket(Integer.parseInt(args[0]));
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

            System.out.println("Starting the LocalMQ Server...");
            server.serve();
        } else {
            System.out.println("Please provide a port for the LocalMQ Server.");
            System.out.println("    " + SimpleMQServer.class.getSimpleName() + " <port>");
        }
    }
}
