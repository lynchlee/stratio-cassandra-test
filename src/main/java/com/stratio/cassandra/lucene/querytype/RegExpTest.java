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
import com.datastax.driver.core.exceptions.InvalidQueryException;

@RunWith(JUnit4.class)
public class RegExpTest extends AbstractWatchedTest {

	@Test
	public void regexpAsciiFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("ascii_1", "frase.*", null));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void regexpAsciiFieldTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("ascii_1", "frase .*", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void regexpAsciiFieldTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("ascii_1", ".*", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void regexpAsciiFieldTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("ascii_1", "", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void regexpAsciiFieldTest5() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("ascii_1", "frase tipo ascii", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void regexpInetFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("inet_1", ".*", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void regexpInetFieldTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("inet_1", "127.*", null));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void regexpInetFieldTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("inet_1", "127.1.*", null));

		

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void regexpInetFieldTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("inet_1", "", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void regexpInetFieldTest5() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("inet_1", "127.1.1.1", null));

		

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	// FIXME TSocketException!
	        public void
	        regexpTextFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("text_1", ".*", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void regexpTextFieldTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("text_1", "frase.*", null));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void regexpTextFieldTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("text_1", "frase .*", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void regexpTextFieldTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("text_1", "", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void regexpTextFieldTest5() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("text_1",
		                                                                         "Frase con espacios articulos y las palabras suficientes",
		                                                                         null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void regexpVarcharFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("varchar_1", ".*", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void regexpVarcharFieldTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("varchar_1", "frase.*", null));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void regexpVarcharFieldTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("varchar_1", "frase .*", null));

		

		assertEquals("Expected 1 results!", 1, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void regexpVarcharFieldTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("varchar_1", "", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void regexpVarcharFieldTest5() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRegexpQuery("varchar_1",
		                                                                         "frasesencillasinespacios",
		                                                                         null));

		

		assertEquals("Expected 1 results!", 1, rows.size());
	}
}
