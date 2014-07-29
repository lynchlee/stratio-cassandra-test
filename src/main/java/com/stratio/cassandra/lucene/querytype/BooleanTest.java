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
package com.stratio.cassandra.lucene.querytype;

/**
 * Created by Jcalderin on 24/03/14.
 */

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.Row;

@RunWith(JUnit4.class)
public class BooleanTest extends AbstractWatchedTest {

	@Test
	public void booleanMustTest() {

		Map<String, String> query1 = new LinkedHashMap<>();
		query1.put("type", "wildcard");
		query1.put("value", "frase*");
		query1.put("field", "ascii_1");
		Map<String, String> query2 = new LinkedHashMap<>();
		query2.put("type", "wildcard");
		query2.put("value", "127.0.*");
		query2.put("field", "inet_1");

		List<Map<String, String>> subqueries = new LinkedList<>();
		subqueries.add(query1);
		subqueries.add(query2);

		List<Row> rows = cassandraUtils.execute(queryUtils.getBooleanQuery(BooleanSubqueryType.MUST, subqueries, null));

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void booleanShouldTest() {

		Map<String, String> query1 = new LinkedHashMap<>();
		query1.put("type", "wildcard");
		query1.put("value", "frase*");
		query1.put("field", "ascii_1");
		Map<String, String> query2 = new LinkedHashMap<>();
		query2.put("type", "wildcard");
		query2.put("value", "127.0.*");
		query2.put("field", "inet_1");

		List<Map<String, String>> subqueries = new LinkedList<>();
		subqueries.add(query1);
		subqueries.add(query2);

		List<Row> rows = cassandraUtils.execute(queryUtils.getBooleanQuery(BooleanSubqueryType.SHOULD, subqueries, null));

		assertEquals("Expected 6 results!", 6, rows.size());
	}

	@Test
	public void booleanMustAndNotTest() {

		Map<String, String> query1 = new LinkedHashMap<>();
		query1.put("type", "wildcard");
		query1.put("value", "frase*");
		query1.put("field", "ascii_1");
		Map<String, String> query2 = new LinkedHashMap<>();
		query2.put("type", "wildcard");
		query2.put("value", "127.0.*");
		query2.put("field", "inet_1");

		List<Map<String, String>> subqueries = new LinkedList<>();
		subqueries.add(query1);
		subqueries.add(query2);

		Map<String, String> query3 = new LinkedHashMap<>();
		query3.put("type", "match");
		query3.put("value", "127.0.0.1");
		query3.put("field", "inet_1");

		List<Map<String, String>> notQueries = new LinkedList<>();
		notQueries.add(query3);

		List<Row> rows = cassandraUtils.execute(queryUtils.getBooleanQuery(BooleanSubqueryType.MUST,
		                                                                   subqueries,
		                                                                   notQueries));

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void booleanShouldAndNotTest() {

		Map<String, String> query1 = new LinkedHashMap<>();
		query1.put("type", "wildcard");
		query1.put("value", "frase*");
		query1.put("field", "ascii_1");
		Map<String, String> query2 = new LinkedHashMap<>();
		query2.put("type", "wildcard");
		query2.put("value", "127.0.*");
		query2.put("field", "inet_1");

		List<Map<String, String>> subqueries = new LinkedList<>();
		subqueries.add(query1);
		subqueries.add(query2);

		Map<String, String> query3 = new LinkedHashMap<>();
		query3.put("type", "match");
		query3.put("value", "127.0.0.1");
		query3.put("field", "inet_1");

		List<Map<String, String>> notQueries = new LinkedList<>();
		notQueries.add(query3);

		List<Row> rows = cassandraUtils.execute(queryUtils.getBooleanQuery(BooleanSubqueryType.SHOULD,
		                                                                   subqueries,
		                                                                   notQueries));

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void booleanMustWithBoostTest() {

		cassandraUtils.execute(queryUtils.getInsert(QueryTypeDataHelper.data5),
		                       queryUtils.getInsert(QueryTypeDataHelper.data6),
		                       queryUtils.getInsert(QueryTypeDataHelper.data7));

		Map<String, String> query1 = new LinkedHashMap<>();
		query1.put("type", "match");
		query1.put("value", "frase");
		query1.put("field", "text_1");
		query1.put("boost", "0.9");
		Map<String, String> query2 = new LinkedHashMap<>();
		query2.put("type", "fuzzy");
		query2.put("value", "127.1.0.1");
		query2.put("field", "inet_1");
		query2.put("boost", "0.0");

		List<Map<String, String>> subqueries = new LinkedList<>();
		subqueries.add(query1);
		subqueries.add(query2);

		List<Row> firstRows = cassandraUtils.execute(queryUtils.getBooleanQuery(BooleanSubqueryType.MUST,
		                                                                        subqueries,
		                                                                        null));

		assertEquals("Expected 3 results!", 3, firstRows.size());

		// Modifying boost values
		query1.put("boost", "0.0");
		query2.put("boost", "0.9");

		List<Row> secondRows = cassandraUtils.execute(queryUtils.getBooleanQuery(BooleanSubqueryType.MUST,
		                                                                         subqueries,
		                                                                         null));

		assertEquals("Expected 3 results!", 3, secondRows.size());

		Row firstSetFirstRow = firstRows.get(0);
		Row secondSetFirstRow = secondRows.get(0);

		assertNotEquals("Expected different values!",
		                firstSetFirstRow.getInt("integer_1"),
		                secondSetFirstRow.getInt("integer_1"));
	}

}
