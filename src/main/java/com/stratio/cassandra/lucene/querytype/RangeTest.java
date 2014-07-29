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
import com.stratio.cassandra.lucene.TestingConstants;

@RunWith(JUnit4.class)
public class RangeTest extends AbstractWatchedTest {

	@Test
	public void rangeAsciiFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("ascii_1", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeAsciiFieldTest2() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "g");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("ascii_1", params));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void rangeAsciiFieldTest3() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "b");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("ascii_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeAsciiFieldTest4() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "f");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("ascii_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeAsciiFieldTest5() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "f");
		params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
		params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("ascii_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeIntegerTest1() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "-5");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "5");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("integer_1", params));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void rangeIntegerTest2() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "-4");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "4");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("integer_1", params));

		

		assertEquals("Expected 3 results!", 3, rows.size());
	}

	@Test
	public void rangeIntegerTest3() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "-4");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "-1");
		params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
		params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("integer_1", params));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void rangeIntegerTest4() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("integer_1", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeBigintTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("bigint_1", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeBigintTest2() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "999999999999999");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "1000000000000001");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("bigint_1", params));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void rangeBigintTest3() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "1000000000000000");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "2000000000000000");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("bigint_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeBigintTest4() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "1000000000000000");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "2000000000000000");
		params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
		params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("bigint_1", params));

		

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void rangeBigintTest5() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "1");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "3");
		params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
		params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("bigint_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeBlobTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("blob_1", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeBlobTest2() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "0x3E0A15");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "0x3E0A17");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("blob_1", params));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void rangeBlobTest3() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "0x3E0A16");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "0x3E0A17");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("blob_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeBlobTest4() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "0x3E0A16");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "0x3E0A17");
		params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
		params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("blob_1", params));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void rangeBlobTest5() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "0x3E0A17");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "0x3E0A18");
		params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
		params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("blob_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeBooleanTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("boolean_1", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeDecimalTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("decimal_1", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeDecimalTest2() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "1999999999.9");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "2000000000.1");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("decimal_1", params));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void rangeDecimalTest3() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "2000000000.0");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "3000000000.0");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("decimal_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeDecimalTest4() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "2000000000.0");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "3000000000.0");
		params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
		params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("decimal_1", params));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void rangeDecimalTest5() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "2000000000.000001");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "2000000000.181235");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("decimal_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeDoubleTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("double_1", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeDoubleTest2() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "1.9");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "2.1");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("double_1", params));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void rangeDoubleTest3() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "2.0");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "3.0");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("double_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeDoubleTest4() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "2.0");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "3.0");
		params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
		params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("double_1", params));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void rangeDoubleTest5() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "7.0");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "10.0");
		params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
		params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("double_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeFloatTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("float_1", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeFloatTest2() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "1.9");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "2.1");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("float_1", params));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void rangeFloatTest3() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "1.0");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "2.0");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("float_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeFloatTest4() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "1.0");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "2.0");
		params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
		params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("float_1", params));

		

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void rangeFloatTest5() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "7.0");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "9.0");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("float_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeUuidTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("uuid_1", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeUuidTest2() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "1");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "9");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("uuid_1", params));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeUuidTest3() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "60297440-b4fa-11e3-8b5a-0002a5d5c51c");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "60297440-b4fa-11e3-8b5a-0002a5d5c51d");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("uuid_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeUuidTest4() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "60297440-b4fa-11e3-8b5a-0002a5d5c51c");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
		params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
		params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("uuid_1", params));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void rangeTimeuuidTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("timeuuid_1", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeTimeuuidTest2() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "z");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("timeuuid_1", params));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeTimeuuidTest3() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a4a70900-24e1-11df-8924-001ff3591712");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "a4a70900-24e1-11df-8924-001ff3591713");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("timeuuid_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeTimeuuidTest4() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a4a70900-24e1-11df-8924-001ff3591712");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "a4a70900-24e1-11df-8924-001ff3591713");
		params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
		params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("timeuuid_1", params));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void rangeInetFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("inet_1", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeInetFieldTest2() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "127.0.0.0");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "127.1.0.0");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("inet_1", params));

		

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void rangeInetFieldTest3() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "127.0.0.0");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "127.1.0.0");
		params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
		params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("inet_1", params));

		

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void rangeInetFieldTest4() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "192.168.0.0");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "192.168.0.1");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("inet_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeTextFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("text_1", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeTextFieldTest2() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "Frase");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "G");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("text_1", params));

		

		assertEquals("Expected 3 results!", 3, rows.size());
	}

	@Test
	public void rangeTextFieldTest3() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT,
		           "Frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "G");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("text_1", params));

		

		assertEquals("Expected 1 result!", 1, rows.size());
	}

	@Test
	public void rangeTextFieldTest4() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT,
		           "Frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "G");
		params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
		params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("text_1", params));

		

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void rangeTextFieldTest5() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "G");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "H");
		params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
		params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("text_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeVarcharFieldTest1() {

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("varchar_1", null));

		

		assertEquals("Expected 5 results!", 5, rows.size());
	}

	@Test
	public void rangeVarcharFieldTest2() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "frase");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "g");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("varchar_1", params));

		

		assertEquals("Expected 4 results!", 4, rows.size());
	}

	@Test
	public void rangeVarcharFieldTest3() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "frasesencillasinespaciosperomaslarga");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "g");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("varchar_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}

	@Test
	public void rangeVarcharFieldTest4() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "frasesencillasinespaciosperomaslarga");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "g");
		params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
		params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("varchar_1", params));

		

		assertEquals("Expected 2 results!", 2, rows.size());
	}

	@Test
	public void rangeVarcharFieldTest5() {

		Map<String, String> params = new LinkedHashMap<>();
		params.put(TestingConstants.LOWER_PARAM_CONSTANT, "g");
		params.put(TestingConstants.UPPER_PARAM_CONSTANT, "h");

		List<Row> rows = cassandraUtils.execute(queryUtils.getRangeQuery("varchar_1", params));

		

		assertEquals("Expected 0 results!", 0, rows.size());
	}
}
