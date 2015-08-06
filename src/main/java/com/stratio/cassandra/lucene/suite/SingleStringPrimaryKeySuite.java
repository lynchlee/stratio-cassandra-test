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
package com.stratio.cassandra.lucene.suite;

import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.querytype.*;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import java.util.Properties;

@RunWith(Suite.class)
@SuiteClasses({AllTest.class,
               FuzzyTest.class,
               WildcardTest.class,
               MatchTest.class,
               NoneTest.class,
               PrefixTest.class,
               PhraseTest.class,
               RegexpTest.class,
               RangeTest.class,
               BooleanTest.class,
               SortTest.class
              })
public class SingleStringPrimaryKeySuite {

    private static CassandraUtils cassandraUtils;

    @BeforeClass
    public static void before() {

        cassandraUtils =
                CassandraUtils.builder()
                              .withTable(TestingConstants.TABLE_NAME_CONSTANT)
                              .withIndexColumn(TestingConstants.INDEX_COLUMN_CONSTANT)
                              .withPartitionKey("ascii_1")
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

        // Add to context ready for suite's tests usage
        Properties context = new Properties();
        context.put("cassandraUtils", cassandraUtils);
        System.setProperties(context);
    }

    @AfterClass
    public static void after() {
        cassandraUtils.disconnect();
    }
}
