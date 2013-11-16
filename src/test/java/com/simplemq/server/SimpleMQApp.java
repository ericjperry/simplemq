package com.simplemq.server;

import com.simplemq.thrift.SimpleMQMessage;
import com.simplemq.thrift.SimpleMQPollResult;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.Scanner;

public class SimpleMQApp {
    public static void main(String[] args) throws TException {
        SimpleMQServer.LocalMQHandler handler = new SimpleMQServer.LocalMQHandler();
        int timeout = 1000;
        String consumerId = "sample_app";
        Scanner keyboard = new Scanner(System.in);
        String mode = "";

        while (!mode.equals("q")) {
            while (!(mode.equals("b") || mode.equals("r") || mode.equals("q") || mode.equals("u"))) {
                System.out.print("Broadcast(b), receive(r), or unsubscribe(u)? ");
                mode = keyboard.nextLine();
            }
            if (mode.equals("b")) {
                System.out.print("Enter a topic: ");
                String topic = keyboard.nextLine();
                System.out.print("Enter a message: ");
                String message = keyboard.nextLine();
                handler.send(new SimpleMQMessage(topic, ByteBuffer.wrap(message.getBytes())));
                mode = "";
            } else if (mode.equals("r")) {
                System.out.print("Enter a topic: ");
                String topic = keyboard.nextLine();
                SimpleMQPollResult result = handler.poll(consumerId, topic, timeout);
                SimpleMQMessage message = result.getMessage();
                if (message == null) {
                    System.out.println("no message");
                } else {
                    System.out.println(new String(message.getData()));
                }
                mode = "";
            } else if (mode.equals("u")) {
                System.out.print("Enter a topic: ");
                String topic = keyboard.nextLine();
                handler.unsubscribeFromTopic(consumerId, topic);
                mode = "";
            }
        }
    }
}
