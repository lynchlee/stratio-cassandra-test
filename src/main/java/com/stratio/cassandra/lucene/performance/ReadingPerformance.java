/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.performance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ReadingPerformance {

    private final static int NUMBER_OF_THREADS = 48;
    private final static int NUMBER_OF_TASKS   = 100;

    public static void main(String[] args) {

        Cluster cluster = Cluster.builder().addContactPoints("172.31.11.69", "172.31.10.149", "172.31.6.56").build();
        Session session = cluster.connect();

        ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);

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
        System.out.println("AVERAGE QUERY LATENCY " +
                           ((double) MyRunnable.totalQueryTime / (double) numQueries) +
                           "ms");
        System.out.println("AVERAGE QUERY TIME " + ((double) time / (double) numQueries) + "ms");
        System.out.println("QUERIES PER SECOND " + ((double) numQueries / (((double) time) / 1000D)));

        executor.shutdownNow();

        System.exit(1);
    }
}
