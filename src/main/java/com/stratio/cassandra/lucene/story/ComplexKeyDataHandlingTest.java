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

import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.index.query.builder.SearchBuilders.wildcard;
import static com.stratio.cassandra.index.query.builder.SearchBuilders.range;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class ComplexKeyDataHandlingTest {

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
                                       .createTable()
                                       .insert(StoryDataHelper.data1)
                                       .insert(StoryDataHelper.data2)
                                       .insert(StoryDataHelper.data3)
                                       .insert(StoryDataHelper.data6)
                                       .insert(StoryDataHelper.data7)
                                       .insert(StoryDataHelper.data8)
                                       .insert(StoryDataHelper.data9)
                                       .insert(StoryDataHelper.data10)
                                       .insert(StoryDataHelper.data11)
                                       .insert(StoryDataHelper.data12)
                                       .insert(StoryDataHelper.data13)
                                       .insert(StoryDataHelper.data14)
                                       .insert(StoryDataHelper.data15)
                                       .insert(StoryDataHelper.data16)
                                       .insert(StoryDataHelper.data17)
                                       .insert(StoryDataHelper.data18)
                                       .insert(StoryDataHelper.data19)
                                       .insert(StoryDataHelper.data20)
                                       .createIndex(TestingConstants.INDEX_NAME_CONSTANT)
                                       .waitForIndexRefresh();
    }

    @After
    public void after() {
        cassandraUtils.dropTable().dropKeyspace().disconnect();
    }

    @Test
    public void singleInsertion() {

        // Data4 insertion
        cassandraUtils.insert(StoryDataHelper.data4).waitForIndexRefresh();
        int n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 19 results!", 19, n);

        // Data5 insertion
        cassandraUtils.insert(StoryDataHelper.data5).waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 20 results!", 20, n);

        // Data4 removal
        cassandraUtils.deleteByCondition("integer_1 = 4 and ascii_1 = 'ascii' and double_1 = 1").waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 19 results!", 19, n);

        // Data5 removal
        cassandraUtils.deleteByCondition("integer_1 = 5 and ascii_1 = 'ascii' and double_1 = 1").waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 18 results!", 18, n);

        // Data2 removal
        cassandraUtils.deleteByCondition("integer_1 = 2 and ascii_1 = 'ascii' and double_1 = 1").waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 17 results!", 17, n);

        // Data3 removal
        cassandraUtils.deleteByCondition("integer_1 = 3 and ascii_1 = 'ascii' and double_1 = 1").waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 16 results!", 16, n);

        // Data1 removal
        cassandraUtils.deleteByCondition("integer_1 = 1 and ascii_1 = 'ascii' and double_1 = 1").waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 15 results!", 15, n);
    }

    @Test
    public void multipleInsertion() {

        cassandraUtils.insert(StoryDataHelper.data4).insert(StoryDataHelper.data5).waitForIndexRefresh();
        int n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 20 results!", 20, n);

        // Data4 removal
        cassandraUtils.deleteByCondition("integer_1 = 4 and ascii_1 = 'ascii' and double_1 = 1").waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 19 results!", 19, n);

        // Data5 removal
        cassandraUtils.deleteByCondition("integer_1 = 5 and ascii_1 = 'ascii' and double_1 = 1").waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 18 results!", 18, n);

        // Data2 removal
        cassandraUtils.deleteByCondition("integer_1 = 2 and ascii_1 = 'ascii' and double_1 = 1").waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 17 results!", 17, n);

        // Data3 removal
        cassandraUtils.deleteByCondition("integer_1 = 3 and ascii_1 = 'ascii' and double_1 = 1").waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 16 results!", 16, n);

        // Data1 removal
        cassandraUtils.deleteByCondition("integer_1 = 1 and ascii_1 = 'ascii' and double_1 = 1").waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 15 results!", 15, n);
    }

    @Test
    public void multipleDeletion() {

        // Data2 & data3 removal
        cassandraUtils.deleteByCondition("integer_1 = 2 and ascii_1 = 'ascii' and double_1 = 1")
                      .deleteByCondition("integer_1 = 3 and ascii_1 = 'ascii' and double_1 = 1")
                      .waitForIndexRefresh();

        int n = cassandraUtils.query(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 16 results!", 16, n);

        // Data1 removal
        cassandraUtils.deleteByCondition("integer_1 = 1 and ascii_1 = 'ascii' and double_1 = 1").waitForIndexRefresh();

        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 15 results!", 15, n);
    }

    @Test
    public void updateTest() {
        int n = cassandraUtils.query(wildcard("text_1", "text")).count();
        assertEquals("Expected 18 results!", 18, n);

        cassandraUtils.update()
                      .set("text_1", "other")
                      .where("integer_1", 4)
                      .and("ascii_1", "ascii")
                      .and("double_1", 1)
                      .execute()
                      .waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("text_1", "text")).count();
        assertEquals("Expected 17 results!", 17, n);
        n = cassandraUtils.query(wildcard("text_1", "other")).count();
        assertEquals("Expected 1 results!", 1, n);
    }
}
