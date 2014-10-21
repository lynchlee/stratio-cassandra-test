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
import com.stratio.cassandra.lucene.util.IndexHandlingUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static com.stratio.cassandra.index.query.builder.SearchBuilders.wildcard;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(JUnit4.class)
public class SimpleKeyDataHandlingTest {

    private CassandraUtils cassandraUtils;

    @Before
    public void before() {

        cassandraUtils = CassandraUtils.builder()
                                       .withTable(TestingConstants.TABLE_NAME_CONSTANT)
                                       .withIndexColumn(TestingConstants.INDEX_COLUMN_CONSTANT)
                                       .withPartitionKey("integer_1")
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

        List<Row> rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 4 results!", 4, rows.size());

        // Data5 insertion
        cassandraUtils.insert(StoryDataHelper.data5).waitForIndexRefresh();

        rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 5 results!", 5, rows.size());

        // Data4 removal
        cassandraUtils.deleteByCondition("integer_1 = 4").waitForIndexRefresh();

        rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 4 results!", 4, rows.size());
        assertFalse("Element not expected!", IndexHandlingUtils.containsElementByIntegerKey(rows, 4));

        // Data5 removal
        cassandraUtils.deleteByCondition("integer_1 = 5").waitForIndexRefresh();

        rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 3 results!", 3, rows.size());
        assertFalse("Element not expected!", IndexHandlingUtils.containsElementByIntegerKey(rows, 5));

        // Data2 removal
        cassandraUtils.deleteByCondition("integer_1 = 2").waitForIndexRefresh();

        rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 2 results!", 2, rows.size());
        assertFalse("Element not expected!", IndexHandlingUtils.containsElementByIntegerKey(rows, 2));

        // Data3 removal
        cassandraUtils.deleteByCondition("integer_1 = 3").waitForIndexRefresh();

        rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 1 result!", 1, rows.size());
        assertFalse("Element not expected!", IndexHandlingUtils.containsElementByIntegerKey(rows, 3));

        // Data1 removal
        cassandraUtils.deleteByCondition("integer_1 = 1").waitForIndexRefresh();

        rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 0 results!", 0, rows.size());
        assertFalse("Element not expected!", IndexHandlingUtils.containsElementByIntegerKey(rows, 1));
    }

    @Test
    public void multipleInsertion() {

        // Data4 and data5 insertion
        cassandraUtils.insert(StoryDataHelper.data4).insert(StoryDataHelper.data5).waitForIndexRefresh();

        List<Row> rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 5 results!", 5, rows.size());

        // Data4 removal
        cassandraUtils.deleteByCondition("integer_1 = 4").waitForIndexRefresh();

        rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 4 results!", 4, rows.size());
        assertFalse("Element not expected!", IndexHandlingUtils.containsElementByIntegerKey(rows, 4));

        // Data5 removal
        cassandraUtils.deleteByCondition("integer_1 = 5").waitForIndexRefresh();

        rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 3 results!", 3, rows.size());
        assertFalse("Element not expected!", IndexHandlingUtils.containsElementByIntegerKey(rows, 5));

        // Data2 removal
        cassandraUtils.deleteByCondition("integer_1 = 2").waitForIndexRefresh();

        rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 2 results!", 2, rows.size());
        assertFalse("Element not expected!", IndexHandlingUtils.containsElementByIntegerKey(rows, 2));

        // Data3 removal
        cassandraUtils.deleteByCondition("integer_1 = 3").waitForIndexRefresh();

        rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 1 result!", 1, rows.size());
        assertFalse("Element not expected!", IndexHandlingUtils.containsElementByIntegerKey(rows, 3));

        // Data1 removal
        cassandraUtils.deleteByCondition("integer_1 = 1").waitForIndexRefresh();

        rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 0 results!", 0, rows.size());
        assertFalse("Element not expected!", IndexHandlingUtils.containsElementByIntegerKey(rows, 1));
    }

    @Test
    public void multipleDeletion() {

        // Data2 & data3 removal
        cassandraUtils.deleteByCondition("integer_1 = 2").deleteByCondition("integer_1 = 3").waitForIndexRefresh();

        List<Row> rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 1 result!", 1, rows.size());
        assertFalse("Element not expected!", IndexHandlingUtils.containsElementByIntegerKey(rows, 3));

        // Data1 removal
        cassandraUtils.deleteByCondition("integer_1 = 1").waitForIndexRefresh();

        rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 0 results!", 0, rows.size());
        assertFalse("Element not expected!", IndexHandlingUtils.containsElementByIntegerKey(rows, 1));
    }
}
