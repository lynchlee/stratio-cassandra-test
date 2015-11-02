/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.cassandra.lucene.bitemporal;

import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.querytype.AbstractWatchedTest;
import com.stratio.cassandra.lucene.schema.SchemaBuilder;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.CassandraUtilsSelect;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.bitemporalMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.schema;
import static com.stratio.cassandra.lucene.search.SearchBuilders.bitemporalSearch;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class FutureBitemporalTests {

    private static final Logger logger = Logger.getLogger(AbstractWatchedTest.class);

    private static long startingTime;
    private static final String TIMESTAMP_PATTERN = "timestamp";
    private static final String SIMPLE_DATE_PATTERN = "yyyy/MM/dd HH:mm:ss.SSS";

    protected static CassandraUtils cassandraUtils;

    public static final Map<String, String> data1;
    public static final Map<String, String> data2;
    public static final Map<String, String> data3;

    static {

        data1 = new LinkedHashMap<>();
        data1.put("integer_1", "1");
        //yyyy/MM/dd HH:mm:ss.SSS
        data1.put("vt_from", "'2015/01/01 00:00:00.000'");
        data1.put("vt_to", "'2200/01/01 00:00:00.000'");
        data1.put("tt_from", "'2015/01/01 12:00:00.001'");
        data1.put("tt_to", "'2015/01/05 12:00:00.000'");

        data2 = new LinkedHashMap<>();
        data2.put("integer_1", "2");
        data2.put("vt_from", "'2015/01/01 12:00:00.001'");
        data2.put("vt_to", "'2015/01/05 12:00:00.000'");
        data2.put("tt_from", "'2015/01/05 12:00:00.001'");
        data2.put("tt_to", "'2015/01/10 12:00:00.000'");

        data3 = new LinkedHashMap<>();
        data3.put("integer_1", "3");
        data3.put("vt_from", "'2015/01/05 12:00:00.001'");
        data3.put("vt_to", "'2200/01/01 00:00:00.000'");
        data3.put("tt_from", "'2015/01/10 12:00:00.001'");
        data3.put("tt_to", "'2200/01/01 00:00:00.000'");

    }

    @Rule
    public TestRule watcher = new TestWatcher() {

        protected void starting(Description description) {
            logger.info("***************** Running test [" + description.getMethodName() + "]");
            startingTime = System.currentTimeMillis();
        }

        protected void finished(Description description) {
            logger.info("***************** Tested in [" + (System.currentTimeMillis() - startingTime) + " ms]");
        }
    };

    @BeforeClass
    public static void setUpSuite() throws InterruptedException {

        SchemaBuilder schemaBuilder = schema().mapper("bitemporal",
                                                      bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").pattern(
                                                              "yyyy/MM/dd HH:mm:ss.SSS")
                                                                                                              .nowValue(
                                                                                                                      "2200/01/01 00:00:00.000"));

        cassandraUtils = CassandraUtils.builder()
                                       .withHost(TestingConstants.CASSANDRA_LOCALHOST_CONSTANT)
                                       .withTable(TestingConstants.TABLE_NAME_CONSTANT)
                                       .withIndexColumn(TestingConstants.INDEX_COLUMN_CONSTANT)
                                       .withPartitionKey("integer_1")
                                       .withClusteringKey()
                                       .withColumn("integer_1", "int")
                                       .withColumn("vt_from", "text")
                                       .withColumn("vt_to", "text")
                                       .withColumn("tt_from", "text")
                                       .withColumn("tt_to", "text")
                                       .withColumn("lucene", "text")
                                       .build();

        cassandraUtils.createKeyspace()
                      .createTable()
                      .createIndex(TestingConstants.INDEX_NAME_CONSTANT, schemaBuilder)
                      .insert(data1, data2, data3);
        System.out.println("finished seting up the testSuite");
    }

    /*@AfterClass
    public static void tearDownSuite() {
        cassandraUtils.dropIndex(TestingConstants.INDEX_NAME_CONSTANT)

                      .dropTable()

                      .dropKeyspace();

        System.out.println("tearDownSuite");
    }*/

    private String fromInteger(Integer[] list) {

        String out = "{";
        for (Integer aList : list) {
            out += Integer.toString(aList) + ",";
        }
        return out.substring(0, out.length() - 1) + "}";

    }

    private boolean isThisAndOnlyThis(Integer[] intList1, int[] intList2) {
        if (intList1.length != intList2.length) {
            return false;
        } else {

            for (Integer i : intList1) {
                boolean found = false;
                for (Integer j : intList2) {
                    if (i.equals(j)) {
                        found = true;
                    }
                }
                if (!found) return false;
            }
            return true;
        }
    }

    @Test
    public void testFuture() {

        CassandraUtilsSelect select = cassandraUtils.filter(bitemporalSearch("bitemporal").ttFrom(
                "2015/01/02 12:00:00.001").ttTo("2015/01/02 12:00:00.001")).refresh(true);

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {1}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1}));
    }

    @Test
    public void testFuture2() {

        CassandraUtilsSelect select = cassandraUtils.filter(bitemporalSearch("bitemporal").ttFrom(
                "2015/01/06 12:00:00.001").ttTo("2015/01/06 12:00:00.001")).refresh(true);

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{2}));
    }

    @Test
    public void testFuture3() {

        CassandraUtilsSelect select = cassandraUtils.filter(bitemporalSearch("bitemporal").ttFrom(
                "2015/01/15 12:00:00.001").ttTo("2015/01/15 12:00:00.001")).refresh(true);

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{3}));
    }

    @Test
    public void testFuture4() {

        CassandraUtilsSelect select = cassandraUtils.filter(bitemporalSearch("bitemporal").vtFrom(
                "2016/01/15 12:00:00.001").vtTo("2016/01/15 12:00:00.001")).refresh(true);

        assertEquals("Expected 2 results!", 2, select.count());
        assertTrue("Unexpected results!! Expected: {1,3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 3}));
    }

    @Test
    public void testFuture5() {

        CassandraUtilsSelect select = cassandraUtils.filter(bitemporalSearch("bitemporal").vtFrom(
                "2015/06/15 12:00:00.001")
                                                                                          .vtTo("2015/07/15 12:00:00.001")
                                                                                          .ttFrom("2015/01/02 12:00:00.001")
                                                                                          .ttTo("2015/01/02 12:00:00.001"))
                                                    .refresh(true);

        assertEquals("Expected 1 result!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {1}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1}));
    }

    @Test
    public void testFuture6() {

        CassandraUtilsSelect select = cassandraUtils.filter(bitemporalSearch("bitemporal").ttFrom(
                "2200/01/01 00:00:00.000").ttTo("2200/01/01 00:00:00.000")).refresh(true);

        assertEquals("Expected 1 result!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{3}));
    }

}
