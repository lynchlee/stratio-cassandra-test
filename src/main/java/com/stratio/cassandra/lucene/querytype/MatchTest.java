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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;

@RunWith(JUnit4.class)
public class MatchTest extends AbstractWatchedTest {

	@Test
	public void matchAsciiFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("ascii_1", "frase tipo ascii", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void matchAsciiFieldTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("ascii_1", "frase", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchAsciiFieldTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("ascii_1", "frase tipo asciii", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void matchAsciiFieldTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("ascii_1", "", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchAsciiFieldTest5() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("ascii_1", "frase tipo asci", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchBigintTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("bigint_1", "1000000000000000", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void matchBigintTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("bigint_1", "3000000000000000", null));

		

		assertEquals("Expected 3 results!", 3, rows.size());
	}

	@Test
	public void matchBigintTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("bigint_1", "10000000000000000", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchBigintTest5() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("bigint_1", "100000000000000", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void matchBlobTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("blob_1", "", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchBlobTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("blob_1", "3E0A16", null));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void matchBlobTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("blob_1", "3E0A161", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchBlobTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("blob_1", "3E0A1", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchBlobTest5() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("blob_1", "3E0A15", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void matchBooleanTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("boolean_1", "", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchBooleanTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("boolean_1", "true", null));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void matchBooleanTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("boolean_1", "false", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void matchBooleanTest5() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("boolean_1", "else", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchDecimalTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("decimal_1", "3000000000.0", null));

		

		assertEquals("Expected 3 results!", 3, rows.size());
	}

	@Test
	public void matchDecimalTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("decimal_1", "300000000.0", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchDecimalTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("decimal_1", "3000000000.00", null));

		

		assertEquals("Expected 3 results!", 3, rows.size());
	}

	@Test
	public void matchDecimalTest5() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("decimal_1", "1000000000.0", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void matchDateTest1() {

		DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		Date date = new Date();

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("date_1", df.format(date), null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchDateTest2() {

		DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, -1);
		Date date = calendar.getTime();

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("date_1", df.format(date), null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchDateTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("date_1",
		                                                                        "1970/01/01 00:00:00.000",
		                                                                        null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchDateTest4() {

		DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, -1);
		Date date = calendar.getTime();

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("date_1", df.format(date), null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchDoubleTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("double_1", "0", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchDoubleTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("double_1", "2.0", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void matchDoubleTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("double_1", "2", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void matchDoubleTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("double_1", "2.00", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void matchFloatTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("float_1", "0", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchFloatTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("float_1", "2.0", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void matchFloatTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("float_1", "2", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void matchFloatTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("float_1", "2.00", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void matchIntegerTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("integer_1", "-2", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void matchIntegerTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("integer_1", "2", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchIntegerTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("integer_1", "0", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchIntegerTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("integer_1", "-1", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void matchUuidTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("uuid_1", "0", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchUuidTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("uuid_1",
		                                                                        "60297440-b4fa-11e3-8b5a-0002a5d5c51b",
		                                                                        null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void matchUuidTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("uuid_1",
		                                                                        "60297440-b4fa-11e3-0002a5d5c51b",
		                                                                        null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchTimeuuidTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("timeuuid_1", "0", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchTimeuuidTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("timeuuid_1",
		                                                                        "a4a70900-24e1-11df-8924-001ff3591711",
		                                                                        null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void matchTimeuuidTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("timeuuid_1",
		                                                                        "a4a70900-24e1-11df-001ff3591711",
		                                                                        null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchInetFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("inet_1", "127.1.1.1", null));

		

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void matchInetFieldTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("inet_1", "127.0.1.1", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void matchInetFieldTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("inet_1", "127.0.1.1.", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void matchInetFieldTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("inet_1", "", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchTextFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("text_1", "Frase", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void matchTextFieldTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("text_1", "Frase*", null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void matchTextFieldTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("text_1",
		                                                                        "Frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga",
		                                                                        null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void matchTextFieldTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("text_1", "", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchVarcharFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("varchar_1",
		                                                                        "frasesencillasinespaciosperomaslarga",
		                                                                        null));

		

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void matchVarcharFieldTest2() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("varchar_1", "frase*", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void matchVarcharFieldTest3() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("varchar_1",
		                                                                        "frasesencillasinespacios",
		                                                                        null));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test(expected = InvalidQueryException.class)
	public void matchVarcharFieldTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getMatchQuery("varchar_1", "", null));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}
}
