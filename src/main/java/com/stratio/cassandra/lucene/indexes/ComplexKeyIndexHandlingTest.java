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
package com.stratio.cassandra.lucene.indexes;

import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.indexes.IndexesDataHelper.*;
import static com.stratio.cassandra.lucene.search.SearchBuilders.wildcard;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class ComplexKeyIndexHandlingTest {

    private CassandraUtils cassandraUtils;

    @Before
    public void before() {

        cassandraUtils = CassandraUtils.builder()
                                       .withHost(TestingConstants.CASSANDRA_LOCALHOST_CONSTANT)
                                       .withTable(TestingConstants.TABLE_NAME_CONSTANT)
                                       .withIndexColumn(TestingConstants.INDEX_COLUMN_CONSTANT)
                                       .withPartitionKey("integer_1", "ascii_1")
                                       .withClusteringKey("double_1")
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
                                       .build()
                                       .createKeyspace()
                                       .createTable();
    }

    @After
    public void after() {
        cassandraUtils.dropTable().dropKeyspace().disconnect();
    }

    @Test
    public void createIndexAfterInsertionsTest() {

        cassandraUtils.insert(data1,
                              data2,
                              data3,
                              data4,
                              data5,
                              data6,
                              data7,
                              data8,
                              data9,
                              data10,
                              data11,
                              data12,
                              data13,
                              data14,
                              data15,
                              data16,
                              data17,
                              data18,
                              data19,
                              data20)
                      .createIndex(TestingConstants.INDEX_NAME_CONSTANT)
                      .waitForIndexing()
                      .refreshIndex();

        // Checking data
        int n = cassandraUtils.filter(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 20 results!", 20, n);
    }

    @Test
    public void createIndexDuringInsertionsTest1() {

        cassandraUtils.insert(data1,
                              data2,
                              data3,
                              data4,
                              data5,
                              data6,
                              data7,
                              data8,
                              data9,
                              data10,
                              data11,
                              data12,
                              data13,
                              data14,
                              data15)
                      .createIndex(TestingConstants.INDEX_NAME_CONSTANT)
                      .waitForIndexing()
                      .insert(data16, data17, data18, data19, data20)
                      .refreshIndex();

        // Checking data
        int n = cassandraUtils.filter(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 20 results!", 20, n);
    }

    @Test
    public void createIndexDuringInsertionsTest2() {

        cassandraUtils.insert(data1,
                              data2,
                              data3,
                              data4,
                              data6,
                              data7,
                              data8,
                              data9,
                              data11,
                              data12,
                              data13,
                              data14,
                              data16,
                              data17,
                              data18,
                              data19)
                      .createIndex(TestingConstants.INDEX_NAME_CONSTANT)
                      .waitForIndexing()
                      .refreshIndex()
                      .insert(data5, data10, data15, data20)
                      .refreshIndex();

        // Checking data
        int n = cassandraUtils.filter(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 20 results!", 20, n);
    }

    @Test
    public void createIndexDuringInsertionsTest3() {

        cassandraUtils.insert(data2,
                              data3,
                              data4,
                              data5,
                              data6,
                              data8,
                              data9,
                              data10,
                              data11,
                              data12,
                              data14,
                              data15,
                              data16,
                              data17,
                              data18,
                              data20)
                      .createIndex(TestingConstants.INDEX_NAME_CONSTANT)
                      .waitForIndexing()
                      .insert(data1, data7, data13, data19)
                      .refreshIndex();

        // Checking data
        int n = cassandraUtils.filter(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 20 results!", 20, n);
    }

    @Test
    public void recreateIndexAfterInsertionsTest() {

        // Creating index
        cassandraUtils.createIndex(TestingConstants.INDEX_NAME_CONSTANT)
                      .waitForIndexing()
                      .insert(data1,
                              data2,
                              data3,
                              data4,
                              data5,
                              data6,
                              data7,
                              data8,
                              data9,
                              data10,
                              data11,
                              data12,
                              data13,
                              data14,
                              data15,
                              data16,
                              data17,
                              data18,
                              data19,
                              data20)
                      .refreshIndex();

        // Checking data
        int n = cassandraUtils.filter(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 20 results!", 20, n);

        // Dropping index
        cassandraUtils.dropIndex(TestingConstants.INDEX_NAME_CONSTANT);

        // Recreating index
        cassandraUtils.createIndex(TestingConstants.INDEX_NAME_CONSTANT).waitForIndexing().refreshIndex();

        // Checking data
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 20 results!", 20, n);
    }
}