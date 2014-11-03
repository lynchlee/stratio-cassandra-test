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
package com.stratio.cassandra.lucene.varia;

import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.stratio.cassandra.index.query.builder.SearchBuilders.match;
import static org.junit.Assert.assertEquals;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SearchWithVeryWideRowsTest {

    private static CassandraUtils cassandraUtils;

    @BeforeClass
    public static void before() {

        cassandraUtils = CassandraUtils.builder()
                                       .withTable(TestingConstants.TABLE_NAME_CONSTANT)
                                       .withIndexColumn(TestingConstants.INDEX_COLUMN_CONSTANT)
                                       .withPartitionKey("partition")
                                       .withClusteringKey("id")
                                       .withColumn("partition", "int")
                                       .withColumn("id", "int")
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
                                       .waitForIndexRefresh();
        int count = 0;
        for (Integer p = 0; p < 2; p++) {
            for (Integer i = 1; i <= 10000; i++) {
                Map<String, String> data = new LinkedHashMap<>();
                data.put("partition", p.toString());
                data.put("id", i.toString());
                data.put("ascii_1", "'ascii_bis'");
                data.put("bigint_1", "3000000000000000");
                data.put("blob_1", "0x3E0A15");
                data.put("boolean_1", "true");
                data.put("decimal_1", "3000000000.0");
                data.put("date_1", String.valueOf(System.currentTimeMillis()));
                data.put("double_1", "2.0");
                data.put("float_1", "3.0");
                data.put("integer_1", "3");
                data.put("inet_1", "'127.1.1.1'");
                data.put("text_1", "'text'");
                data.put("varchar_1", "'varchar'");
                data.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
                data.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591713");
                data.put("list_1", "['l2','l3']");
                data.put("set_1", "{'s2','s3'}");
                data.put("map_1", "{'k2':'v2','k3':'v3'}");
                cassandraUtils.insert(data);
            }
        }
        System.out.println("INSERTIONS " + count);
        cassandraUtils.waitForIndexRefresh();
    }

    @AfterClass
    public static void after() {
        cassandraUtils.dropIndex(TestingConstants.INDEX_NAME_CONSTANT).dropTable().dropKeyspace().waitForIndexRefresh();
    }

    @Test
    public void search4999Test() {
        int n = cassandraUtils.filter(match("partition", 0)).limit(4999).count();
        assertEquals("Expected 4999 results!", 4999, n);
    }

    @Test
    public void search5000Test() {
        int n = cassandraUtils.filter(match("partition", 0)).limit(5000).count();
        assertEquals("Expected 5000 results!", 5000, n);
    }

    @Test
    public void search5001Test() {
        int n = cassandraUtils.filter(match("partition", 0)).limit(5001).count();
        assertEquals("Expected 4999 results!", 5001, n);
    }

    @Test
    public void search9999Test() {
        int n = cassandraUtils.filter(match("partition", 0)).limit(9999).count();
        assertEquals("Expected 9999 results!", 9999, n);
    }

    @Test
    public void search10000Test() {
        int n = cassandraUtils.filter(match("partition", 0)).limit(10000).count();
        assertEquals("Expected 10000 results!", 10000, n);
    }

    @Test
    public void search10001Test() {
        int n = cassandraUtils.filter(match("partition", 0)).limit(10001).count();
        assertEquals("Expected 10000 results!", 10000, n);
    }

    @Test
    public void searchAll10001Test() {
        int n = cassandraUtils.searchAll().limit(10001).count();
        assertEquals("Expected 10001 results!", 10001, n);
    }

    @Test
    public void searchAll14999Test() {
        int n = cassandraUtils.searchAll().limit(14999).count();
        assertEquals("Expected 14999 results!", 14999, n);
    }

    @Test
    public void searchAll15000Test() {
        int n = cassandraUtils.searchAll().limit(15000).count();
        assertEquals("Expected 15000 results!", 15000, n);
    }

    @Test
    public void searchAll15001Test() {
        int n = cassandraUtils.searchAll().limit(15001).count();
        assertEquals("Expected 15001 results!", 15001, n);
    }
}
