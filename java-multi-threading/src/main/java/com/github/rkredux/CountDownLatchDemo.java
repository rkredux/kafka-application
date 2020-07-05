package com.github.rkredux;

import java.util.concurrent.CountDownLatch;

public class CountDownLatchDemo {

    public static void main(String[] args) {

        CountDownLatch latch = new CountDownLatch(4);
        Worker worker1 = new Worker("WORKER1", 1000, latch);
        Worker worker2 = new Worker("WORKER2", 2000, latch);
        Worker worker3 = new Worker("WORKER3", 3000, latch);
        Worker worker4 = new Worker("WORKER4", 4000, latch);

        worker1.start();
        worker2.start();
        worker3.start();
        worker4.start();

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Starting " + Thread.currentThread().getName() + " thread");
        System.out.println(Thread.currentThread().getId() + " has finished running");
    }
}

class Worker extends Thread{
    private CountDownLatch latch;
    String name;
    private int delay;

    public Worker(String name,Integer delay, CountDownLatch latch){
        super(name);
        this.delay = delay;
        this.latch = latch;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(delay);
            latch.countDown();
            System.out.println(Thread.currentThread().getName() + " just finished running");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
