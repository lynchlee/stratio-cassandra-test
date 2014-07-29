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

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.Row;

@RunWith(JUnit4.class)
public class PrefixTest extends AbstractWatchedTest {

	@Test
	public void prefixAsciiFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("ascii_1", "frase ", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void prefixAsciiFieldTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("ascii_1", "frase", null));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void prefixAsciiFieldTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("ascii_1", "F", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void prefixAsciiFieldTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("ascii_1", "", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void prefixInetFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("inet_1", "127", null));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void prefixInetFieldTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("inet_1", "", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void prefixInetFieldTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("inet_1", "127.0.", null));

		

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void prefixTextFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("text_1",
		                                                                         "Frase con espacios articulos y las palabras suficientes",
		                                                                         null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void prefixTextFieldTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("text_1", "Frase", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void prefixTextFieldTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("text_1", "", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void prefixVarcharFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("varchar_1",
		                                                                         "frasesencillasinespaciosperomaslarga",
		                                                                         null));

		

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void prefixVarcharFieldTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("varchar_1", "frase", null));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void prefixVarcharFieldTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getPrefixQuery("varchar_1", "", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}
}
