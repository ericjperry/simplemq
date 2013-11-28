package com.simplemq.client;

import com.google.common.base.Optional;
import org.apache.thrift.transport.TTransportException;

import java.util.Scanner;

public class SimpleMQClientApp {
    public static void main(String[] args) throws TTransportException {
        int timeout = 1000;
        String consumerId = "sample_app";
        Scanner keyboard = new Scanner(System.in);
        String mode = "";
        SimpleMQClient client = new SimpleMQClient("localhost", 19090, consumerId, timeout);

        while (!mode.equals("q")) {
            while (!(mode.equals("b") || mode.equals("r") || mode.equals("q"))) {
                System.out.print("Broadcast(b) or receive(r)? ");
                mode = keyboard.nextLine();
            }
            if (mode.equals("b")) {
                System.out.print("Enter a topic: ");
                String topic = keyboard.nextLine();
                System.out.print("Enter a message: ");
                String message = keyboard.nextLine();
                client.send(topic, message.getBytes());
                mode = "";
            } else if (mode.equals("r")) {
                System.out.print("Enter a topic: ");
                String topic = keyboard.nextLine();
                Optional<byte[]> result = client.poll(topic);
                if (result.isPresent()) {
                    System.out.println(new String(result.get()));
                } else {
                    System.out.println("no message");
                }
                mode = "";
            }
        }
    }
}
