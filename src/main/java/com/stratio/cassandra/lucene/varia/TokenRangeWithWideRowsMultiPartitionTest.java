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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@RunWith(JUnit4.class)
public class TokenRangeWithWideRowsMultiPartitionTest {

    private static CassandraUtils cassandraUtils;

    @BeforeClass
    public static void before() {

        cassandraUtils = CassandraUtils.builder()
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
                                       .createIndex(TestingConstants.INDEX_NAME_CONSTANT)
                                       .insert(VariaDataHelper.data1)
                                       .insert(VariaDataHelper.data2)
                                       .insert(VariaDataHelper.data3)
                                       .insert(VariaDataHelper.data4)
                                       .insert(VariaDataHelper.data5)
                                       .insert(VariaDataHelper.data6)
                                       .insert(VariaDataHelper.data7)
                                       .insert(VariaDataHelper.data8)
                                       .insert(VariaDataHelper.data9)
                                       .insert(VariaDataHelper.data10)
                                       .insert(VariaDataHelper.data11)
                                       .insert(VariaDataHelper.data12)
                                       .insert(VariaDataHelper.data13)
                                       .insert(VariaDataHelper.data14)
                                       .insert(VariaDataHelper.data15)
                                       .insert(VariaDataHelper.data16)
                                       .insert(VariaDataHelper.data17)
                                       .insert(VariaDataHelper.data18)
                                       .insert(VariaDataHelper.data19)
                                       .insert(VariaDataHelper.data20)
                                       ;
    }

    @AfterClass
    public static void after() {
//        cassandraUtils.dropIndex(TestingConstants.INDEX_NAME_CONSTANT).dropTable().dropKeyspace();
    }

    @Test
    public void tokenSearchTest1() {
        int n = cassandraUtils.searchAll().and("AND TOKEN(integer_1, ascii_1) > TOKEN(1, 'ascii')").count();
        assertEquals("Expected 8 results!", 8, n);
    }

    @Test
    public void tokenSearchTest2() {
        int n = cassandraUtils.searchAll().and("AND TOKEN(integer_1, ascii_1) >= TOKEN(1, 'ascii')").count();
        assertEquals("Expected 10 results!", 10, n);
    }

    @Test
    public void tokenSearchTest3() {
        int n = cassandraUtils.searchAll().and("AND TOKEN(integer_1, ascii_1) < TOKEN(1, 'ascii')").count();
        assertEquals("Expected 10 results!", 10, n);
    }

    @Test
    public void tokenSearchTest4() {
        int n = cassandraUtils.searchAll().and("AND TOKEN(integer_1, ascii_1) <= TOKEN(1, 'ascii')").count();
        assertEquals("Expected 12 results!", 12, n);
    }

    @Test
    public void tokenSearchTest5() {
        int n = cassandraUtils.searchAll()
                              .and("AND TOKEN(integer_1, ascii_1) > TOKEN(1, 'ascii')")
                              .and("AND TOKEN(integer_1, ascii_1) < TOKEN(3, 'ascii')")
                              .count();
        assertEquals("Expected 6 results!", 6, n);
    }

    @Test
    public void tokenSearchTest6() {
        int n = cassandraUtils.searchAll()
                              .and("AND TOKEN(integer_1, ascii_1) >= TOKEN(1, 'ascii')")
                              .and("AND TOKEN(integer_1, ascii_1) < TOKEN(3, 'ascii')")
                              .count();
        assertEquals("Expected 8 results!", 8, n);
    }

    @Test
    public void tokenSearchTest7() {
        int n = cassandraUtils.searchAll()
                              .and("AND TOKEN(integer_1, ascii_1) > TOKEN(1, 'ascii')")
                              .and("AND TOKEN(integer_1, ascii_1) <= TOKEN(3, 'ascii')")
                              .count();
        assertEquals("Expected 8 results!", 8, n);
    }

    @Test
    public void tokenSearchTest8() {
        int n = cassandraUtils.searchAll()
                              .and("AND TOKEN(integer_1, ascii_1) >= TOKEN(1, 'ascii')")
                              .and("AND TOKEN(integer_1, ascii_1) <= TOKEN(3, 'ascii')")
                              .count();
        assertEquals("Expected 10 results!", 10, n);
    }

    @Test
    public void tokenSearchTest9() {
        int n = cassandraUtils.searchAll()
                              .and("AND TOKEN(integer_1, ascii_1) = TOKEN(1, 'ascii')")
                              .count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void tokenSearchTest10() {
        int n = cassandraUtils.searchAll()
                              .count();
        assertEquals("Expected 20 results!", 20, n);
    }

    @Test
    public void tokenClusteringSearchTest1() {
        int n = cassandraUtils.searchAll()
                              .and("AND integer_1 = 1 AND ascii_1 = 'ascii_bis' AND double_1 > 1")
                              .count();
        assertEquals("Expected 1 results!", 1, n);
    }

    @Test
    public void tokenClusteringSearchTest2() {
        int n = cassandraUtils.searchAll()
                              .and("AND integer_1 = 1 AND ascii_1 = 'ascii_bis' AND double_1 >= 1").count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void tokenClusteringSearchTest3() {
        int n = cassandraUtils.searchAll()
                              .and("AND integer_1 = 1 AND ascii_1 = 'ascii_bis' AND double_1 < 2").count();
        assertEquals("Expected 1 results!", 1, n);
    }

    @Test
    public void tokenClusteringSearchTest4() {
        int n = cassandraUtils.searchAll()
                              .and("AND integer_1 = 1 AND ascii_1 = 'ascii_bis' AND double_1 <= 2").count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void tokenClusteringSearchTest5() {
        int n = cassandraUtils.searchAll()
                              .and("AND integer_1 = 1 AND ascii_1 = 'ascii_bis' AND double_1 = 1").count();
        assertEquals("Expected 1 results!", 1, n);
    }

    @Test
    public void tokenClusteringSearchTest6() {
        int n = cassandraUtils.searchAll()
                              .and("AND integer_1 = 1 AND ascii_1 = 'ascii_bis' AND double_1 = 2").count();
        assertEquals("Expected 1 results!", 1, n);
    }
}
