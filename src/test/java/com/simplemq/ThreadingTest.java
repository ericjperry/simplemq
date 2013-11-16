package com.simplemq;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.simplemq.core.SimpleMessageQueue;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 * User: eperry
 * Date: 11/15/13
 * Time: 9:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class ThreadingTest {
    private static class Producer implements Runnable {
        SimpleMessageQueue mq;
        int steps;
        int range;
        int start;

        public Producer(SimpleMessageQueue mq, int start, int range, int steps) {
            this.mq = mq;
            this.start = start;
            this.steps = steps;
            this.range = range;
        }

        @Override
        public void run() {
            int count = start;
            while (count < range) {
                ByteBuffer buf = ByteBuffer.allocate(4);
                buf.putInt(count);
                count += steps;
                mq.enqueue("test", buf.array());
            }
        }
    }

    private static class Consumer implements Runnable {
        SimpleMessageQueue mq;

        public Consumer(SimpleMessageQueue mq) {
            this.mq = mq;
        }

        @Override
        public void run() {
            Optional<byte[]> result;
            boolean done = false;
            boolean foundOne = false;
            while (!done) {
                result = mq.dequeue("consumer", "test");
                if (result.isPresent()) {
                    System.out.println(ByteBuffer.wrap(result.get()).getInt());
                    foundOne = true;
                } else if (foundOne) {
                    done = true;
                }
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Scanner keyboard = new Scanner(System.in);
        System.out.print("Range: ");
        int range = Integer.parseInt(keyboard.nextLine());
        System.out.print("Producer threads: ");
        int pThreads = Integer.parseInt(keyboard.nextLine());
        System.out.print("Consumer threads: ");
        int cThreads = Integer.parseInt(keyboard.nextLine());
        SimpleMessageQueue q = new SimpleMessageQueue();

        ExecutorService service = Executors.newFixedThreadPool(pThreads + cThreads);

        for (int i = 0; i < pThreads; i++) {
            service.execute(new Producer(q, i, range, pThreads));
        }
        for (int i = 0; i < cThreads; i++) {
            service.execute(new Consumer(q));
        }

        service.shutdown();
        System.out.println("done");
    }
}
