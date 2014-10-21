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
package com.stratio.cassandra.lucene.util;

import com.stratio.cassandra.lucene.TestingConstants;

import java.util.*;

public class CassandraUtilsBuilder {

    private String host  = TestingConstants.CASSANDRA_LOCALHOST_CONSTANT;
    private String table = TestingConstants.TABLE_NAME_CONSTANT;
    private Map<String, String>       columns;
    private List<String> partitionKey;
    private List<String> clusteringKey;
    private String indexColumn = TestingConstants.INDEX_COLUMN_CONSTANT;

    CassandraUtilsBuilder() {
        super();
        this.columns = new HashMap<>();
        this.partitionKey = new ArrayList<>();
        this.clusteringKey = new ArrayList<>();
    }

    public CassandraUtilsBuilder withHost(String host) {
        this.host = host;
        return this;
    }

    public CassandraUtilsBuilder withTable(String table) {
        this.table = table;
        return this;
    }

    public CassandraUtilsBuilder withIndexColumn(String indexColumn) {
        this.indexColumn = indexColumn;
        return this;
    }

    public CassandraUtilsBuilder withColumn(String name, String type) {
        columns.put(name, type);
        return this;
    }

    public CassandraUtilsBuilder withPartitionKey(String... columns) {
        partitionKey.addAll(Arrays.asList(columns));
        return this;
    }

    public CassandraUtilsBuilder withClusteringKey(String... columns) {
        clusteringKey.addAll(Arrays.asList(columns));
        return this;
    }

    public CassandraUtils build() {
        String keyspace = "test_lucene_" +
                          new Random().nextInt(Integer.MAX_VALUE - 1) +
                          "" +
                          new Random().nextInt(Integer.MAX_VALUE - 1);
        return new CassandraUtils(host, keyspace, table, columns, partitionKey, clusteringKey, indexColumn);
    }
}
