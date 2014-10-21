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
package com.stratio.cassandra.lucene.breakdown;

/**
 * Created by Jcalderin on 24/03/14.
 */

import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class BreakdownTest {

    private static final Logger logger = Logger.getLogger(BreakdownTest.class);

    private static CassandraUtils cassandraUtils;

    @BeforeClass
    public static void setUpSuite() {

        cassandraUtils =
                CassandraUtils.builder()
                              .withHost(TestingConstants.CASSANDRA_LOCALHOST_CONSTANT)
                              .withTable(TestingConstants.TABLE_NAME_CONSTANT)
                              .withIndexColumn(TestingConstants.INDEX_COLUMN_CONSTANT)
                              .withPartitionKey("integer_1")
                              .withClusteringKey()
                              .withColumn("ascii_1", "ascii")
                              .withColumn("bigint_1", "bigint")
                              .withColumn("blob_1", "blob")
                              .withColumn("boolean_1", "boolean")
                              .withColumn("decimal_1", "decimal")
                              .withColumn("date_1", "timestamp")
                              .withColumn("double_1", "double")
                              .withColumn("float_1", "float")
                              .withColumn("integer_1", "int")
                              .withColumn("inet_1", "inet")
                              .withColumn("text_1", "text")
                              .withColumn("varchar_1", "varchar")
                              .withColumn("uuid_1", "uuid")
                              .withColumn("timeuuid_1", "timeuuid")
                              .withColumn("list_1", "list<text>")
                              .withColumn("set_1", "set<text>")
                              .withColumn("map_1", "map<text,text>")
                              .withColumn("lucene", "text")
                              .build();

        // Executing db queries
        cassandraUtils.createKeyspace()
                      .createTable()
                      .createIndex(TestingConstants.INDEX_NAME_CONSTANT)
                      .insert(QueryTypeDataHelper.data1)
                      .insert(QueryTypeDataHelper.data2)
                      .insert(QueryTypeDataHelper.data3)
                      .insert(QueryTypeDataHelper.data4);
    }

    @AfterClass
    public static void tearDownSuite() {
        logger.debug("Dropping keyspace");
        cassandraUtils.dropKeyspace().disconnect();
    }

    @Test
    public void breakdownTest1() {

        Map<String, String> data4Modified = new LinkedHashMap<>(QueryTypeDataHelper.data4);
        data4Modified.put("double_1", "40");

        // TODO Kill node2
        Process p;
        try {
            logger.info("Stopping the node1 before modifying data...");
            p = Runtime.getRuntime().exec("ccm node1 stop");
            p.waitFor();
            logger.info("Node1 stopped!");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        cassandraUtils.insert(data4Modified);
        logger.info("Data modified");

        // TODO Recover node2
        try {
            logger.info("Starting the node1...");
            p = Runtime.getRuntime().exec("ccm node1 start");
            p.waitFor();
            logger.info("Node1 started, let's repair it...");
            p = Runtime.getRuntime().exec("ccm node1 repair");
            p.waitFor();
            logger.info("Node1 repaired!");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        List<Row> rows = cassandraUtils.selectAllFromTable();

        boolean modified = false;
        for (int i = 0; i < rows.size(); i++) {
            double rowDouble1 = rows.get(i).getDouble("double_1");
            if (rowDouble1 == 40) {
                modified = true;
            }
        }

        assertTrue("Value wasn't modified!", modified);
    }
}
