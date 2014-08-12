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
package com.stratio.cassandra.lucene.suite;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.querytype.BooleanTest;
import com.stratio.cassandra.lucene.querytype.FuzzyTest;
import com.stratio.cassandra.lucene.querytype.MatchTest;
import com.stratio.cassandra.lucene.querytype.PhraseTest;
import com.stratio.cassandra.lucene.querytype.PrefixTest;
import com.stratio.cassandra.lucene.querytype.RangeTest;
import com.stratio.cassandra.lucene.querytype.RegExpTest;
import com.stratio.cassandra.lucene.querytype.WildcardTest;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.QueryUtils;
import com.stratio.cassandra.lucene.util.QueryUtilsBuilder;

@RunWith(Suite.class)
@SuiteClasses({ FuzzyTest.class, WildcardTest.class, MatchTest.class, PrefixTest.class, PhraseTest.class,
        RegExpTest.class, RangeTest.class, BooleanTest.class })
public class SingleNumericPrimaryKeySuite {

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

		Properties context = new Properties();
		context.put("queryUtils", queryUtils);
		context.put("cassandraUtils", cassandraUtils);

		// Adding testing needed objects
		System.setProperties(context);
	};

	@AfterClass
	public static void tearDownSuite() {
		cassandraUtils.disconnect();
	};
}
