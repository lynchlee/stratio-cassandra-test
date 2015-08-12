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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;

public class MyRunnable implements Runnable {

    private static final Logger logger = Logger.getLogger(MyRunnable.class);

    public static long numQueries;
    public static long totalQueryTime;

    private String name;
    private static Session session;

    public MyRunnable(String name, Session session) {
        this.name = name;

        if (MyRunnable.session == null) MyRunnable.session = session;

    }

    @Override
    public void run() {

        List<String> queries = new LinkedList<>();

        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{filter:{type:\"range\", field:\"createdat\", lower:\"2014-01-01\", upper:\"2014-03-01\"}}' limit 10;");
        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{query:{type:\"match\", field:\"message\", value:\"dios\"}}' limit 10;");
        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{query:{type:\"match\", field:\"message\", value:\"dios\"}, filter:{type:\"range\", field:\"createdat\", lower:\"2014-01-01\", upper:\"2014-03-01\"}}' limit 10;");

        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{filter:{type:\"range\", field:\"createdat\", lower:\"2014-02-01\", upper:\"2014-03-01\"}}' limit 10;");
        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{query:{type:\"match\", field:\"message\", value:\"hola\"}}' limit 10;");
        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{query:{type:\"match\", field:\"message\", value:\"hola\"}, filter:{type:\"range\", field:\"createdat\", lower:\"2014-01-01\", upper:\"2014-03-01\"}}' limit 10;");

        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{filter:{type:\"range\", field:\"createdat\", lower:\"2014-01-15\", upper:\"2014-03-01\"}}' limit 10;");
        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{query:{type:\"match\", field:\"message\", value:\"gracias\"}}' limit 10;");
        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{query:{type:\"match\", field:\"message\", value:\"gracias\"}, filter:{type:\"range\", field:\"createdat\", lower:\"2014-01-15\", upper:\"2014-03-01\"}}' limit 10;");

        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{filter:{type:\"range\", field:\"createdat\", lower:\"2014-01-16\", upper:\"2014-03-01\"}}' limit 10;");
        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{query:{type:\"match\", field:\"message\", value:\"mapache\"}}' limit 10;");
        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{query:{type:\"match\", field:\"message\", value:\"mapache\"}, filter:{type:\"range\", field:\"createdat\", lower:\"2014-01-01\", upper:\"2014-03-01\"}}' limit 10;");

        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{filter:{type:\"range\", field:\"createdat\", lower:\"2014-01-14\", upper:\"2014-03-02\"}}' limit 10;");
        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{query:{type:\"match\", field:\"message\", value:\"político\"}}' limit 10;");
        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{query:{type:\"match\", field:\"message\", value:\"político\"}, filter:{type:\"range\", field:\"createdat\", lower:\"2014-01-01\", upper:\"2014-03-01\"}}' limit 10;");

        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{filter:{type:\"range\", field:\"createdat\", lower:\"2014-01-14\", upper:\"2014-03-02\"}}' limit 10;");
        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{query:{type:\"wildcard\", field:\"message\", value:\"ha*\"}}' limit 10;");
        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{query:{type:\"wildcard\", field:\"message\", value:\"ha*\"}, filter:{type:\"range\", field:\"createdat\", lower:\"2014-01-01\", upper:\"2014-03-01\"}}' limit 10;");

        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{filter:{type:\"range\", field:\"createdat\", lower:\"2013-01-14\", upper:\"2014-03-02\"}}' limit 10;");
        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{query:{type:\"phrase\", field:\"message\", values:[\"rechazar\",\"la\",\"violencia\"]}}' limit 10;");
        queries.add(
                "SELECT * FROM  twitter.tweets WHERE lucene='{query:{type:\"phrase\", field:\"message\", values:[\"rechazar\",\"la\",\"violencia\"]}, filter:{type:\"range\", field:\"createdat\", lower:\"2014-01-01\", upper:\"2014-03-01\"}}' limit 10;");

        for (String query : queries) {
            long queryStart = System.currentTimeMillis();
            ResultSet rs = session.execute(query);
            long queryTime = System.currentTimeMillis() - queryStart;
            numQueries++;
            totalQueryTime += queryTime;
            logger.info("[" + this.name + "] [" + queryTime + "ms] [" + rs.all().size() + "rows] " + query);
        }
    }

}