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
package com.stratio.cassandra.lucene;

public final class TestingConstants {

    public static final Long WRITE_WAIT_TIME = 6000L;

    public static final Long READ_WAIT_TIME = 0L;

    public static final String DEFAULT_CONSISTENCY_LEVEL = "QUORUM";

    public static final String CASSANDRA_LOCALHOST_CONSTANT = "127.0.0.1";

    public static final String REPLICATION_FACTOR_CONSTANT_NAME = "replicationFactor";

    public static final String CONSISTENCY_LEVEL_CONSTANT_NAME = "consistencyLevel";

    public static final String TABLE_NAME_CONSTANT = "table_index_test";

    public static final String INDEX_NAME_CONSTANT = "lucene_index";

    public static final String INDEX_COLUMN_CONSTANT = "lucene";

    public static final int FETCH_SIZE = 100;

}
