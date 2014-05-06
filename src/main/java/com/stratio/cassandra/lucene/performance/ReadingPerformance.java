package com.stratio.cassandra.lucene.performance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class ReadingPerformance {

    private final static int NUMBER_OF_THREADS = 48;
    private final static int NUMBER_OF_TASKS = 100;

    public static void main(String[] args) {

        Cluster cluster = Cluster
                .builder()
                .addContactPoints("172.31.11.69", "172.31.10.149",
                        "172.31.6.56").build();
        Session session = cluster.connect();

        ExecutorService executor = Executors
                .newFixedThreadPool(NUMBER_OF_THREADS);

        long start = System.currentTimeMillis();

        for (int i = 0; i < NUMBER_OF_TASKS; i++) {

            Runnable worker = new MyRunnable("Thread-" + i, session);
            executor.execute(worker);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }

        long time = System.currentTimeMillis() - start;
        long numQueries = MyRunnable.numQueries;

        System.out.println("TOTAL TIME " + time);
        System.out.println("NUM EXECUTED TASKS " + numQueries);
        System.out.println("AVERAGE QUERY LATENCY "
                + ((double) MyRunnable.totalQueryTime / (double) numQueries)
                + "ms");
        System.out.println("AVERAGE QUERY TIME "
                + ((double) time / (double) numQueries) + "ms");
        System.out.println("QUERIES PER SECOND "
                + ((double) numQueries / (((double) time) / 1000D)));

        executor.shutdownNow();

        System.exit(1);
    }
}
