package com.stratio.cassandra.lucene.performance;

public class ReadingPerformance {

    private final static int NUMBER_OF_THREADS = 20;

    public static void main(String[] args) {

        for (int i = 0; i < NUMBER_OF_THREADS; i++) {

            Thread r = new MyRunnable("Thread-" + i);

            r.start();
        }
    }
}
