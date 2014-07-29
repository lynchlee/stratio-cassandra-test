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

/**
 * Created by Jcalderin on 24/03/14.
 */

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.QueryUtils;
import com.stratio.cassandra.lucene.util.QueryUtilsBuilder;

@RunWith(JUnit4.class)
public class SimpleKeyDataDeletionTest {

	private static final Logger logger = Logger.getLogger(SimpleKeyDataDeletionTest.class);

	private static QueryUtils queryUtils;

	private static CassandraUtils cassandraUtils;

	@BeforeClass
	public static void setUpSuite() {

		// Initializing suite data
		Map<String, String> columns = new LinkedHashMap<String, String>();
		columns.put("ascii_1", "ascii");
		columns.put("bigint_1", "bigint");
		columns.put("blob_1", "blob");
		columns.put("boolean_1", "boolean");
		columns.put("decimal_1", "decimal");
		columns.put("date_1", "timestamp");
		columns.put("double_1", "double");
		columns.put("float_1", "float");
		columns.put("integer_1", "int");
		columns.put("inet_1", "inet");
		columns.put("text_1", "text");
		columns.put("varchar_1", "varchar");
		columns.put("uuid_1", "uuid");
		columns.put("timeuuid_1", "timeuuid");
		columns.put("list_1", "list<text>");
		columns.put("set_1", "set<text>");
		columns.put("map_1", "map<text,text>");
		columns.put("lucene", "text");

		Map<String, List<String>> primaryKey = new LinkedHashMap<String, List<String>>();
		String[] inarray = { "integer_1" };
		String[] outarray = {};
		List<String> in = Arrays.asList(inarray);
		List<String> out = Arrays.asList(outarray);
		primaryKey.put("in", in);
		primaryKey.put("out", out);

		queryUtils = new QueryUtilsBuilder(TestingConstants.TABLE_NAME_CONSTANT,
		                                   columns,
		                                   primaryKey,
		                                   TestingConstants.INDEX_COLUMN_CONSTANT).build();

		cassandraUtils = new CassandraUtils(TestingConstants.CASSANDRA_LOCALHOST_CONSTANT);
	};

	@AfterClass
	public static void tearDownSuite() {

		cassandraUtils.disconnect();
	};

	@Before
	public void setUp() throws InterruptedException {
		cassandraUtils.execute(queryUtils.createKeyspaceQuery(),
		                       queryUtils.createTableQuery(),
		                       queryUtils.createIndex(TestingConstants.INDEX_NAME_CONSTANT),
		                       queryUtils.getInsert(DeletionDataHelper.data1),
		                       queryUtils.getInsert(DeletionDataHelper.data2),
		                       queryUtils.getInsert(DeletionDataHelper.data3),
		                       queryUtils.getInsert(DeletionDataHelper.data4),
		                       queryUtils.getInsert(DeletionDataHelper.data5));
	}

	@After
	public void tearDown() {
		// Dropping keyspace
		logger.debug("Dropping keyspace");
		cassandraUtils.execute(queryUtils.dropKeyspaceQuery());
	}

	@Test
	public void columnDeletion() {

		cassandraUtils.execute(queryUtils.constructValueDeleteQueryByCondition("bigint_1", "integer_1 = 1"));

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("ascii_1", "*", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());

		int integerValue;
		for (Row row : rows) {
			integerValue = row.getInt("integer_1");
			if (integerValue == 1) {
				assertTrue("Must be null!", row.isNull("bigint_1"));
			}
		}
	}

	@Test
	public void mapElementDeletion() {

		cassandraUtils.execute(queryUtils.constructValueDeleteQueryByCondition("map_1['k1']", "integer_1 = 1"));

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("ascii_1", "*", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());

		int integerValue;
		Map<String, String> mapValue = null;
		for (Row row : rows) {
			integerValue = row.getInt("integer_1");
			if (integerValue == 1) {
				mapValue = row.getMap("map_1", String.class, String.class);
			}
		}

		assertNotNull("Must not be null!", mapValue);
		assertNull("Must be null!", mapValue.get("k1"));
	}

	@Test
	public void listElementDeletion() {

		cassandraUtils.execute(queryUtils.constructValueDeleteQueryByCondition("list_1[0]", "integer_1 = 1"));

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("ascii_1", "*", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());

		int integerValue;
		List<String> listValue = null;
		for (Row row : rows) {
			integerValue = row.getInt("integer_1");
			if (integerValue == 1) {
				listValue = row.getList("list_1", String.class);
			}
		}

		assertNotNull("Must not be null!", listValue);
		assertEquals("Lenght unexpected", 1, listValue.size());
	}

	@Test
	public void totalPartitionDeletion() {

		cassandraUtils.execute(queryUtils.constructDeleteQueryByCondition("integer_1 = 1"));

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("ascii_1", "*", null));

		

		assertEquals("Expected 4 results!", 4, rows.size());

	}
}
