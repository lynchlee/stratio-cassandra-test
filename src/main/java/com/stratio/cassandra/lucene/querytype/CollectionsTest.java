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

import java.util.Arrays;
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
public class CollectionsTest extends AbstractWatchedTest {

	@Test(expected = InvalidQueryException.class)
	public void matchListFieldTest1() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("list_1", "", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchListFieldTest2() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("list_1", "l1", null));
		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void matchListFieldTest3() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("list_1", "s1", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void matchSetFieldTest1() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("set_1", "", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchSetFieldTest2() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("set_1", "l1", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchSetFieldTest3() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("set_1", "s1", null));
		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void matchMapFieldTest1() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("map_1.k1", "", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchMapFieldTest2() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("map_1.k1", "l1", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchMapFieldTest3() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("map_1.k1", "k1", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchMapFieldTest4() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("map_1.k1", "v1", null));
		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void fuzzyListFieldTest1() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("list_1", "", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void fuzzyListFieldTest2() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("list_1", "l1", null));
		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void fuzzyListFieldTest3() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("list_1", "s1", null));
		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void fuzzyListFieldTest4() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("list_1", "s7l", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void fuzzySetFieldTest1() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("set_1", "", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void fuzzySetFieldTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("set_1", "l1", null));

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void fuzzySetFieldTest3() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("set_1", "s1", null));
		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void fuzzySetFieldTest4() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("set_1", "k87", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void fuzzyMapFieldTest1() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("map_1.k1", "", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void fuzzyMapFieldTest2() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("map_1.k1", "l1", null));
		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void fuzzyMapFieldTest3() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("map_1.k1", "k1", null));
		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void fuzzyMapFieldTest4() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getFuzzyQuery("map_1.k1", "v1", null));
		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void phraseListFieldTest1() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPhraseQuery("list_1", null, null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void phraseListFieldTest2() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPhraseQuery("list_1", Arrays.asList("l1"), null));
		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void phraseListFieldTest3() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPhraseQuery("list_1", Arrays.asList("l1", "l2"), null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void phraseListFieldTest4() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPhraseQuery("list_1", Arrays.asList("s1"), null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void phraseSetFieldTest1() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPhraseQuery("set_1", null, null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void phraseSetFieldTest2() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPhraseQuery("set_1", Arrays.asList("l1"), null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void phraseSetFieldTest3() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPhraseQuery("set_1", Arrays.asList("s1"), null));
		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void phraseMapFieldTest1() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPhraseQuery("map_1.k1", null, null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void phraseMapFieldTest2() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPhraseQuery("map_1.k1", Arrays.asList("l1"), null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void phraseMapFieldTest3() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPhraseQuery("map_1.k1", Arrays.asList("k1"), null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void phraseMapFieldTest4() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPhraseQuery("map_1.k1", Arrays.asList("v1"), null));
		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void prefixListFieldTest1() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("list_1", "", null));
		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void prefixListFieldTest2() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("list_1", "l1", null));
		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void prefixListFieldTest3() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("list_1", "l", null));
		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void prefixListFieldTest4() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("list_1", "s1", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void prefixSetFieldTest1() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("set_1", "", null));
		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void prefixSetFieldTest2() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("set_1", "l1", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void prefixSetFieldTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("set_1", "s1", null));

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void prefixMapFieldTest1() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("map_1.k1", "", null));
		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void prefixMapFieldTest2() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("map_1.k1", "l1", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void prefixMapFieldTest3() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("map_1.k1", "k1", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void prefixMapFieldTest4() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("map_1.k1", "v1", null));
		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void rangeListFieldTest1() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "z");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("list_1", params));

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeListFieldTest2() {
		
		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a1");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "z9");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("list_1", params));

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeListFieldTest3() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a1");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "l1");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("list_1", params));

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeSetFieldTest1() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "z");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("set_1", params));

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeSetFieldTest2() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a1");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "z9");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("set_1", params));

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeSetFieldTest3() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a1");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "z1");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("set_1", params));

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeMapFieldTest1() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "z");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("map_1.k1", params));

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void rangeMapFieldTest2() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a1");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "z9");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("map_1.k1", params));

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void rangeMapFieldTest3() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a1");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "k9");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("map_1.k1", params));

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeMapFieldTest4() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a1");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "k1");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("map_1.k1", params));

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void regexpListFieldTest1() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("list_1", "", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void regexpListFieldTest2() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("list_1", "l.*", null));
		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void regexpListFieldTest3() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("list_1", "s.*", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void regexpSetFieldTest1() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("set_1", "", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void regexpSetFieldTest2() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("set_1", "l.*", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void regexpSetFieldTest3() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("set_1", "s.*", null));
		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void regexpMapFieldTest1() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("map_1.k1", "", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void regexpMapFieldTest2() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("map_1.k1", "l.*", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void regexpMapFieldTest3() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("map_1.k1", "k.*", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void regexpMapFieldTest4() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("map_1.k1", "v.*", null));
		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void wildcardListFieldTest1() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("list_1", "", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void wildcardListFieldTest2() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("list_1", "l*", null));
		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void wildcardListFieldTest3() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("list_1", "s*", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void wildcardSetFieldTest1() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("set_1", "", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void wildcardSetFieldTest2() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("set_1", "l*", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void wildcardSetFieldTest3() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("set_1", "s*", null));
		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void wildcardMapFieldTest1() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("map_1.k1", "", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void wildcardMapFieldTest2() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("map_1.k1", "l*", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void wildcardMapFieldTest3() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("map_1.k1", "k*", null));
		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void wildcardMapFieldTest4() {
		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("map_1.k1", "v*", null));
		assertEquals("Expected 2 results!", 2, rows.size());
	}
}