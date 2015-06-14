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
public class MultipleKeyDataHandlingTest {

    private static CassandraUtils cassandraUtils;

    @Before
    public void before() {

        cassandraUtils = CassandraUtils.builder()
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
                                       .createTable()
                                       .createIndex(TestingConstants.INDEX_NAME_CONSTANT)
                                       .insert(StoryDataHelper.data1)
                                       .insert(StoryDataHelper.data2)
                                       .insert(StoryDataHelper.data3)
                                       .insert(StoryDataHelper.data6)
                                       .insert(StoryDataHelper.data7)
                                       .insert(StoryDataHelper.data8)
                                       .insert(StoryDataHelper.data9)
                                       .insert(StoryDataHelper.data10)
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

        assertEquals("Expected 9 results!", 9, n);

        // Data5 insertion
        cassandraUtils.insert(StoryDataHelper.data5).waitForIndexRefresh();

        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 10 results!", 10, n);

        // Data4 removal
        cassandraUtils.deleteByCondition("integer_1 = 4 and ascii_1 = 'ascii'").waitForIndexRefresh();

        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 9 results!", 9, n);

        // Data5 removal
        cassandraUtils.deleteByCondition("integer_1 = 5 and ascii_1 = 'ascii'").waitForIndexRefresh();

        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 8 results!", 8, n);

        // Data2 removal
        cassandraUtils.deleteByCondition("integer_1 = 2 and ascii_1 = 'ascii'").waitForIndexRefresh();

        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 7 results!", 7, n);

        // Data3 removal
        cassandraUtils.deleteByCondition("integer_1 = 3 and ascii_1 = 'ascii'").waitForIndexRefresh();

        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 6 results!", 6, n);

        // Data1 removal
        cassandraUtils.deleteByCondition("integer_1 = 1 and ascii_1 = 'ascii'").waitForIndexRefresh();

        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void multipleInsertion() {

        int n;

        // Data4 and data5 insertion
        cassandraUtils.insert(StoryDataHelper.data4).insert(StoryDataHelper.data5).waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 10 results!", 10, n);

        // Data4 removal
        cassandraUtils.deleteByCondition("integer_1 = 4 and ascii_1 = 'ascii'").waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 9 results!", 9, n);

        // Data5 removal
        cassandraUtils.deleteByCondition("integer_1 = 5 and ascii_1 = 'ascii'").waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 8 results!", 8, n);

        // Data2 removal
        cassandraUtils.deleteByCondition("integer_1 = 2 and ascii_1 = 'ascii'").waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 7 results!", 7, n);

        // Data3 removal
        cassandraUtils.deleteByCondition("integer_1 = 3 and ascii_1 = 'ascii'").waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 6 results!", 6, n);

        // Data1 removal
        cassandraUtils.deleteByCondition("integer_1 = 1 and ascii_1 = 'ascii'").waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void multipleDeletion() {

        // Data2 & data3 removal
        cassandraUtils.deleteByCondition("integer_1 = 2 and ascii_1 = 'ascii'")
                      .deleteByCondition("integer_1 = 3 and ascii_1 = 'ascii'")
                      .waitForIndexRefresh();

        int n = cassandraUtils.query(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 6 result!", 6, n);

        // Data1 removal
        cassandraUtils.deleteByCondition("integer_1 = 1 and ascii_1 = 'ascii'").waitForIndexRefresh();

        n = cassandraUtils.query(wildcard("ascii_1", "*")).count();

        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void updateTest() {
        int n = cassandraUtils.query(wildcard("text_1", "text")).count();
        assertEquals("Expected 8 results!", 8, n);

        cassandraUtils.update()
                      .set("text_1", "other")
                      .where("integer_1", 1)
                      .and("ascii_1", "ascii")
                      .execute()
                      .waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("text_1", "text")).count();
        assertEquals("Expected 7 results!", 7, n);
        n = cassandraUtils.query(wildcard("text_1", "other")).count();
        assertEquals("Expected 1 results!", 1, n);
    }

    @Test
    public void insertWithUpdateTest() {
        int n = cassandraUtils.query(wildcard("text_1", "text")).count();
        assertEquals("Expected 8 results!", 8, n);

        cassandraUtils.update()
                      .set("text_1", "new")
                      .where("integer_1", 1000)
                      .and("ascii_1", "ascii")
                      .execute()
                      .waitForIndexRefresh();
        n = cassandraUtils.query(wildcard("text_1", "new")).count();
        assertEquals("Expected 1 results!", 1, n);
    }
}
