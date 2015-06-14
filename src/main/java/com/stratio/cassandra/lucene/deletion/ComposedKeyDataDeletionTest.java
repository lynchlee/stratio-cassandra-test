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
package com.stratio.cassandra.lucene.deletion;

import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.Map;

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.wildcard;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class ComposedKeyDataDeletionTest {

    private CassandraUtils cassandraUtils;

    @Before
    public void before() {

        cassandraUtils = CassandraUtils.builder()
                                       .withHost(TestingConstants.CASSANDRA_LOCALHOST_CONSTANT)
                                       .withTable(TestingConstants.TABLE_NAME_CONSTANT)
                                       .withIndexColumn(TestingConstants.INDEX_COLUMN_CONSTANT)
                                       .withPartitionKey("integer_1", "ascii_1")
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
                                       .build()
                                       .createKeyspace()
                                       .createTable()
                                       .createIndex(TestingConstants.INDEX_NAME_CONSTANT)
                                       .insert(DeletionDataHelper.data1)
                                       .insert(DeletionDataHelper.data2)
                                       .insert(DeletionDataHelper.data3)
                                       .insert(DeletionDataHelper.data4)
                                       .insert(DeletionDataHelper.data5)
                                       .insert(DeletionDataHelper.data6)
                                       .insert(DeletionDataHelper.data7)
                                       .insert(DeletionDataHelper.data8)
                                       .insert(DeletionDataHelper.data9)
                                       .insert(DeletionDataHelper.data10)
                                       .waitForIndexRefresh();
    }

    @After
    public void after() {
        cassandraUtils.dropKeyspace().disconnect();
    }

    @Test
    public void columnDeletion() {

        cassandraUtils.deleteValueByCondition("bigint_1", "integer_1 = 1 and ascii_1 = 'ascii'").waitForIndexRefresh();

        List<Row> rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 10 results!", 10, rows.size());

        int integerValue;
        String asciiValue;
        double doubleValue;
        for (Row row : rows) {
            integerValue = row.getInt("integer_1");
            asciiValue = row.getString("ascii_1");
            doubleValue = row.getDouble("double_1");
            if ((integerValue == 1) && (asciiValue.equals("ascii")) && (doubleValue == 1)) {
                assertTrue("Must be null!", row.isNull("bigint_1"));
            }
        }
    }

    @Test
    public void mapElementDeletion() {

        cassandraUtils.deleteValueByCondition("map_1['k1']", "integer_1 = 1 and ascii_1 = 'ascii'")
                      .waitForIndexRefresh();

        List<Row> rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 10 results!", 10, rows.size());

        int integerValue;
        String asciiValue;
        double doubleValue;
        Map<String, String> mapValue = null;
        for (Row row : rows) {
            integerValue = row.getInt("integer_1");
            asciiValue = row.getString("ascii_1");
            doubleValue = row.getDouble("double_1");
            if ((integerValue == 1) && (asciiValue.equals("ascii")) && (doubleValue == 1)) {
                mapValue = row.getMap("map_1", String.class, String.class);
            }
        }

        assertNotNull("Must not be null!", mapValue);
        assertNull("Must be null!", mapValue.get("k1"));
    }

    @Test
    public void listElementDeletion() {

        cassandraUtils.deleteValueByCondition("list_1[0]", "integer_1 = 1 and ascii_1 = 'ascii'").waitForIndexRefresh();

        List<Row> rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 10 results!", 10, rows.size());

        int integerValue;
        String stringValue;
        List<String> listValue = null;
        for (Row row : rows) {
            integerValue = row.getInt("integer_1");
            stringValue = row.getString("ascii_1");
            if (integerValue == 1 && stringValue.equals("ascii")) {
                listValue = row.getList("list_1", String.class);
            }
        }

        assertNotNull("Must not be null!", listValue);
        assertEquals("Lenght unexpected", 1, listValue.size());
    }

    @Test
    public void totalPartitionDeletion() {

        cassandraUtils.deleteByCondition("integer_1 = 1 and ascii_1 = 'ascii'").waitForIndexRefresh();

        List<Row> rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 9 results!", 9, rows.size());

    }
}