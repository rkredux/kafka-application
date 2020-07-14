package com.github.rkredux;

import java.util.List;

import java.util.ArrayList;

public class MultiExecutorApp {

    private final List<Runnable> tasks;

    public MultiExecutorApp(List<Runnable> tasks) {
        this.tasks = tasks;
    }

    public void executeAll() {
        List<Thread> threads = new ArrayList<>(tasks.size());

        for (Runnable task : tasks) {
            Thread thread = new Thread(task);
            threads.add(thread);
        }

        for(Thread thread : threads) {
            thread.start();
        }
    }
}