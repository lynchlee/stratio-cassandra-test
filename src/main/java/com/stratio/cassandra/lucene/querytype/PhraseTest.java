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

@RunWith(JUnit4.class)
public class PhraseTest extends AbstractWatchedTest {

	@Test()
	public void phraseTextFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getPhraseQuery("text_1",
		                                                                         Arrays.asList("Frase", "espacios"),
		                                                                         null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test()
	public void phraseTextFieldWithSlopTest1() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put("slop", "2");

		List<Row> rows = cassandraUtils.execute(queryUtils.getPhraseQuery("text_1",
		                                                                         Arrays.asList("Frase", "espacios"),
		                                                                         params));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test()
	public void phraseTextFieldTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getPhraseQuery("text_1",
		                                                                         Arrays.asList("articulos",
		                                                                                       "suficientes"),
		                                                                         null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test()
	public void phraseTextFieldWithSlopTest() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put("slop", "2");

		List<Row> rows = cassandraUtils.execute(queryUtils.getPhraseQuery("text_1",
		                                                                         Arrays.asList("articulos", "palabras"),
		                                                                         params));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test()
	public void phraseTextFieldTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getPhraseQuery("text_1",
		                                                                         Arrays.asList("con", "los"),
		                                                                         null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test()
	public void phraseTextFieldTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getPhraseQuery("text_1", null, null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}
}