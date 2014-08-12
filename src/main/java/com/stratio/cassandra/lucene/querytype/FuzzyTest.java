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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.stratio.cassandra.lucene.TestingConstants;

@RunWith(JUnit4.class)
public class FuzzyTest extends AbstractWatchedTest {

	@Test
	public void fuzzyAsciiFieldTest() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("ascii_1", "frase tipo asci", null));

		assertEquals("Expected 2 result!", 2, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void emptyFuzzyAsciiFieldTest() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("ascii_1", "", null));

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void fuzzyAsciiFieldWith1MaxEditsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "1");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("ascii_1", "frase tipo asci", params));

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void fuzzyAsciiFieldWith0MaxEditsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "0");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("ascii_1", "frase tipo asci", params));

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void fuzzyAsciiFieldWith2PrefixLengthTest1() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.PREFIX_LENGTH_PARAM_CONSTANT, "2");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("ascii_1", "frase typo ascii", params));

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void fuzzyAsciiFieldWith2PrefixLengthTest2() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.PREFIX_LENGTH_PARAM_CONSTANT, "2");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("ascii_1", "phrase tipo ascii", params));

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void fuzzyAsciiFieldWith1MaxExpansionsTest() {

		// Adding new data for the test
		cassandraUtils.execute(queryUtils.getInsert(QueryTypeDataHelper.data5));

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EXPANSIONS_PARAM_CONSTANT, "1");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("ascii_1", "frase tipo ascii", params));

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void fuzzyAsciiFieldWith10MaxExpansionsTest() {

		// Adding new data for the test
		cassandraUtils.execute(queryUtils.getInsert(QueryTypeDataHelper.data5));

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EXPANSIONS_PARAM_CONSTANT, "10");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("ascii_1", "frase tipo ascii", params));

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void fuzzyAsciiFieldWithoutTranspositionsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "false");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("ascii_1", "farse itpo ascii", params));

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void fuzzyAsciiFieldWithTranspositionsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("ascii_1", "farse itpo ascii", params));

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void fuzzyAsciiFieldWith5MaxEditsAndTranspositionsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "1");
		params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("ascii_1", "farse itpo ascii", params));

		assertEquals("Expected 0 results!", 0, rows.size());

	}

	@Test
	public void fuzzyInetFieldTest() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("inet_1", "127.0.1.1", null));

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void emptyFuzzyInetFieldTest() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("inet_1", "", null));

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void fuzzyInetFieldWith1MaxEditsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "1");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("inet_1", "127.0.0.1", params));

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void fuzzyInetFieldWith0MaxEditsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "0");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("inet_1", "127.0.1.1", params));

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void fuzzyInetFieldWith2PrefixLengthTest1() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.PREFIX_LENGTH_PARAM_CONSTANT, "2");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("inet_1", "127.0.1.1", params));

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void fuzzyInetFieldWith2PrefixLengthTest2() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.PREFIX_LENGTH_PARAM_CONSTANT, "2");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("inet_1", "117.0.1.1", params));

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void fuzzyInetFieldWith1MaxExpansionsTest() throws InterruptedException {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EXPANSIONS_PARAM_CONSTANT, "1");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("inet_1", "127.0.1.1", params));

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void fuzzyInetFieldWith10MaxExpansionsTest() throws InterruptedException {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EXPANSIONS_PARAM_CONSTANT, "10");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("inet_1", "127.0.1.1", params));

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void fuzzyInetFieldWithoutTranspositionsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "false");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("inet_1", "1270..1.1", params));

		assertEquals("Expected 3 results!", 3, rows.size());
	}

	@Test
	public void fuzzyInetFieldWithTranspositionsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("inet_1", "1270..1.1", params));

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void fuzzyInetFieldWith1MaxEditsAndTranspositionsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "1");
		params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("inet_1", "1270..1.1", params));

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void fuzzyTextFieldTest() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("text_1",
		                                                                 "Frasesinespaciosconarticulosylaspalabrassuficiente",
		                                                                 null));

		assertEquals("Expected 2 result!", 2, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void emptyFuzzyTextFieldTest() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("text_1", "", null));

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void fuzzyTextFieldWith1MaxEditsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "1");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("text_1",
		                                                                 "Frasesinespaciosconarticulosylaspalabrassuficiente",
		                                                                 params));

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void fuzzyTextFieldWith0MaxEditsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "0");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("text_1",
		                                                                 "Frasesinespaciosconarticulosylaspalabrassuficiente",
		                                                                 params));

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void fuzzyTextFieldWith2PrefixLengthTest1() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.PREFIX_LENGTH_PARAM_CONSTANT, "2");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("text_1",
		                                                                 "Frasesinespaciosconarticulosylaspalabrassuficiente",
		                                                                 params));

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void fuzzyTextFieldWith2PrefixLengthTest2() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.PREFIX_LENGTH_PARAM_CONSTANT, "2");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("text_1",
		                                                                 "rFasesinespaciosconarticulosylaspalabrassuficiente",
		                                                                 params));

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void fuzzyTextFieldWith1MaxExpansionsTest() throws InterruptedException {

		// Adding new data for the test
		cassandraUtils.execute(queryUtils.getInsert(QueryTypeDataHelper.data5));

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EXPANSIONS_PARAM_CONSTANT, "1");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("text_1",
		                                                                 "Frasesinespaciosconarticulosylaspalabrassuficiente",
		                                                                 params));

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void fuzzyTextFieldWith10MaxExpansionsTest() throws InterruptedException {

		// Adding new data for the test
		cassandraUtils.execute(queryUtils.getInsert(QueryTypeDataHelper.data5));

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EXPANSIONS_PARAM_CONSTANT, "10");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("text_1",
		                                                                 "Frasesinespaciosconarticulosylaspalabrassuficiente",
		                                                                 params));

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void fuzzyTextFieldWithoutTranspositionsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "false");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("text_1",
		                                                                 "Frasseinespacisoconarticulosylaspalabrassuficientes",
		                                                                 params));

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void fuzzyTextFieldWithTranspositionsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("text_1",
		                                                                 "Frasseinespacisoconarticulosylaspalabrassuficientes",
		                                                                 params));

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void fuzzyTextFieldWith5MaxEditsAndTranspositionsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "1");
		params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("text_1",
		                                                                 "Frasseinespacisoconarticulosylaspalabrassuficientes",
		                                                                 params));

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void fuzzyVarcharFieldTest() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("varchar_1",
		                                                                 "frasesencillasnespaciosperomaslarga",
		                                                                 null));

		assertEquals("Expected 3 results!", 3, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void emptyFuzzyVarcharFieldTest() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("varchar_1", "", null));

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void fuzzyVarcharFieldWith1MaxEditsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "1");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("varchar_1",
		                                                                 "frasesencillasnespaciosperomaslarga",
		                                                                 params));

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void fuzzyVarcharFieldWith0MaxEditsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "0");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("varchar_1",
		                                                                 "frasesencillasnespaciosperomaslarga",
		                                                                 params));

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void fuzzyVarcharFieldWith2PrefixLengthTest1() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.PREFIX_LENGTH_PARAM_CONSTANT, "2");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("varchar_1",
		                                                                 "frasesencillasnespaciosperomaslarga",
		                                                                 params));

		assertEquals("Expected 2 result2!", 2, rows.size());
	}

	@Test
	public void fuzzyVarcharFieldWith2PrefixLengthTest2() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.PREFIX_LENGTH_PARAM_CONSTANT, "2");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("varchar_1",
		                                                                 "rfasesencillasnespaciosperomaslarga",
		                                                                 params));

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void fuzzyVarcharFieldWith1MaxExpansionsTest() throws InterruptedException {

		// Adding new data for the test
		cassandraUtils.execute(queryUtils.getInsert(QueryTypeDataHelper.data5));

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EXPANSIONS_PARAM_CONSTANT, "1");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("varchar_1",
		                                                                 "frasesencillasnespaciosperomaslarga",
		                                                                 params));

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void fuzzyVarcharFieldWith10MaxExpansionsTest() throws InterruptedException {

		// Adding new data for the test
		cassandraUtils.execute(queryUtils.getInsert(QueryTypeDataHelper.data5));

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EXPANSIONS_PARAM_CONSTANT, "10");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("varchar_1",
		                                                                 "frasesencillasnespaciosperomaslarga",
		                                                                 params));

		assertEquals("Expected 3 results!", 3, rows.size());
	}

	@Test
	public void fuzzyVarcharFieldWithoutTranspositionsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "false");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("varchar_1",
		                                                                 "frasesenicllasnespaciosperomaslarga",
		                                                                 params));

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void fuzzyVarcharFieldWithTranspositionsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("varchar_1",
		                                                                 "frasesenicllasnespaciosperomaslarga",
		                                                                 params));

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void fuzzyVarcharFieldWith5MaxEditsAndTranspositionsTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "1");
		params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("varchar_1",
		                                                                 "frasesenicllasnespaciosperomaslarga",
		                                                                 params));

		assertEquals("Expected 0 results!", 0, rows.size());
	}
}
