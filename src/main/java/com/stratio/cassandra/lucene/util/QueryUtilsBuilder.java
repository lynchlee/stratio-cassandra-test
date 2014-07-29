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

import java.util.List;
import java.util.Map;
import java.util.Random;

public class QueryUtilsBuilder {

	private String table;
	private Map<String, String> columns;
	private Map<String, List<String>> primaryKey;
	private String indexColumn;

	public QueryUtilsBuilder(String table,
	                         Map<String, String> columns,
	                         Map<String, List<String>> primaryKey,
	                         String indexColumn) {
		super();
		this.table = table;
		this.columns = columns;
		this.primaryKey = primaryKey;
		this.indexColumn = indexColumn;
	}

	public QueryUtils build() {
		String keyspace = "test_lucene_" + new Random().nextInt(Integer.MAX_VALUE - 1) + ""
		        + new Random().nextInt(Integer.MAX_VALUE - 1);
		return new QueryUtils(keyspace, table, columns, primaryKey, indexColumn);
	}
}
