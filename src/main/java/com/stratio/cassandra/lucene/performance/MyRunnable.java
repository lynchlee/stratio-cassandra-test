package com.stratio.cassandra.lucene.performance;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class MyRunnable extends Thread {

    private static final Logger logger = Logger.getLogger(MyRunnable.class);

    public MyRunnable(String name) {
        super(name);
    }

    @Override
    public void run() {

        Cluster cluster = Cluster
                .builder()
                .addContactPoints("172.31.11.69", "172.31.10.149",
                        "172.31.6.56").build();
        Session session = cluster.connect();

        String query = "SELECT * FROM twitter.tweets limit 10;";
        long startingTime = System.currentTimeMillis();
        logger.info("[" + this.getName() + "] Starting query [" + startingTime
                + "ms]");
        session.execute(query);
        long endingTime = System.currentTimeMillis();
        logger.info("[" + this.getName() + "] Query finished [" + endingTime
                + "ms] It took [" + String.valueOf(endingTime - startingTime)
                + "ms]");
    }

}