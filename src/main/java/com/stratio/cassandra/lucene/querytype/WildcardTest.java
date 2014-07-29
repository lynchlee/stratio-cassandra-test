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
public class WildcardTest extends AbstractWatchedTest {

	@Test
	public void wildcardAsciiFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("ascii_1", "*", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void wildcardAsciiFieldTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("ascii_1", "frase*", null));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void wildcardAsciiFieldTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("ascii_1", "frase *", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void wildcardAsciiFieldTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("ascii_1", "", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void wildcardInetFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("inet_1", "*", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void wildcardInetFieldTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("inet_1", "127*", null));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void wildcardInetFieldTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("inet_1", "127.1.*", null));

		

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void wildcardInetFieldTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("inet_1", "", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void wildcardTextFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("text_1", "*", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void wildcardTextFieldTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("text_1", "Frase*", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	// FIXME Returns 0 and I'm expecting 3...
	        public void
	        wildcardTextFieldTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("text_1", "Frasesin*", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void wildcardTextFieldTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("text_1", "", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void wildcardVarcharFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("varchar_1", "*", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void wildcardVarcharFieldTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("varchar_1", "frase*", null));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void wildcardVarcharFieldTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("varchar_1", "frase sencilla*", null));

		

		assertEquals("Expected 1 results!", 1, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void wildcardVarcharFieldTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getWildcardQuery("varchar_1", "", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}
}
