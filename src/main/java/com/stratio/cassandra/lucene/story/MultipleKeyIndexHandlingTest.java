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
package com.stratio.cassandra.lucene.story;

import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.wildcard;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class MultipleKeyIndexHandlingTest {

    private static CassandraUtils cassandraUtils;

    @Before
    public void before() {

        cassandraUtils =
                CassandraUtils.builder()
                              .withTable(TestingConstants.TABLE_NAME_CONSTANT)
                              .withIndexColumn(TestingConstants.INDEX_COLUMN_CONSTANT)
                              .withPartitionKey("integer_1")
                              .withClusteringKey("ascii_1")
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

        // Preparing data
        cassandraUtils.insert(StoryDataHelper.data1)
                      .insert(StoryDataHelper.data2)
                      .insert(StoryDataHelper.data3)
                      .insert(StoryDataHelper.data4)
                      .insert(StoryDataHelper.data5)
                      .insert(StoryDataHelper.data6)
                      .insert(StoryDataHelper.data7)
                      .insert(StoryDataHelper.data8)
                      .insert(StoryDataHelper.data9)
                      .insert(StoryDataHelper.data10)
                      .createIndex(TestingConstants.INDEX_NAME_CONSTANT)
                      .waitForIndexRefresh();

        // Checking data
        int n = cassandraUtils.query(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 10 results!", 10, n);
    }

    @Test
    public void createIndexDuringInsertionsTest1() {

        // Preparing initial data
        cassandraUtils.insert(StoryDataHelper.data1)
                      .insert(StoryDataHelper.data2)
                      .insert(StoryDataHelper.data3)
                      .insert(StoryDataHelper.data4)
                      .insert(StoryDataHelper.data5)
                      .insert(StoryDataHelper.data6)
                      .insert(StoryDataHelper.data7)
                      .insert(StoryDataHelper.data8)
                      .createIndex(TestingConstants.INDEX_NAME_CONSTANT)
                      .insert(StoryDataHelper.data9)
                      .insert(StoryDataHelper.data10)
                      .waitForIndexRefresh();

        // Checking data
        int n = cassandraUtils.query(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 10 results!", 10, n);
    }

    @Test
    public void createIndexDuringInsertionsTest2() {

        cassandraUtils.insert(StoryDataHelper.data1)
                      .insert(StoryDataHelper.data2)
                      .insert(StoryDataHelper.data3)
                      .insert(StoryDataHelper.data4)
                      .insert(StoryDataHelper.data6)
                      .insert(StoryDataHelper.data7)
                      .insert(StoryDataHelper.data8)
                      .insert(StoryDataHelper.data9)
                      .createIndex(TestingConstants.INDEX_NAME_CONSTANT)
                      .insert(StoryDataHelper.data5)
                      .insert(StoryDataHelper.data10)
                      .waitForIndexRefresh();

        // Checking data
        int n = cassandraUtils.query(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 10 results!", 10, n);
    }

    @Test
    public void createIndexDuringInsertionsTest3() {

        // Preparing initial data
        cassandraUtils.insert(StoryDataHelper.data2)
                      .insert(StoryDataHelper.data3)
                      .insert(StoryDataHelper.data4)
                      .insert(StoryDataHelper.data5)
                      .insert(StoryDataHelper.data6)
                      .insert(StoryDataHelper.data7)
                      .insert(StoryDataHelper.data8)
                      .insert(StoryDataHelper.data9)
                      .createIndex(TestingConstants.INDEX_NAME_CONSTANT)
                      .insert(StoryDataHelper.data1)
                      .insert(StoryDataHelper.data10)
                      .waitForIndexRefresh();

        // Checking data
        int n = cassandraUtils.query(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 10 results!", 10, n);
    }

    @Test
    public void recreateIndexAfterInsertionsTest() {

        cassandraUtils.insert(StoryDataHelper.data1)
                      .insert(StoryDataHelper.data2)
                      .insert(StoryDataHelper.data3)
                      .insert(StoryDataHelper.data4)
                      .insert(StoryDataHelper.data5)
                      .insert(StoryDataHelper.data6)
                      .insert(StoryDataHelper.data7)
                      .insert(StoryDataHelper.data8)
                      .insert(StoryDataHelper.data9)
                      .insert(StoryDataHelper.data10)
                      .createIndex(TestingConstants.INDEX_NAME_CONSTANT)
                      .waitForIndexRefresh();

        int n = cassandraUtils.query(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 10 results!", 10, n);

        // Dropping index
        cassandraUtils.dropIndex(TestingConstants.INDEX_NAME_CONSTANT);

        // Recreating index
        cassandraUtils.createIndex(TestingConstants.INDEX_NAME_CONSTANT).waitForIndexRefresh();

        // Checking data
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 10 results!", 10, n);
    }
}