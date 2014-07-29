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

import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.QueryUtils;
import com.stratio.cassandra.lucene.util.QueryUtilsBuilder;

public abstract class AbstractWatchedTest {

	private static final Logger logger = Logger.getLogger(AbstractWatchedTest.class);

	private static long startingTime;

	protected static CassandraUtils cassandraUtils;
	protected static QueryUtils queryUtils;

	@Rule
	public TestRule watcher = new TestWatcher() {

		protected void starting(Description description) {
			logger.info("***************** Running test [" + description.getMethodName() + "]");
			startingTime = System.currentTimeMillis();
		}

		protected void finished(Description description) {
			logger.info("***************** Tested in [" + (System.currentTimeMillis() - startingTime) + " ms]");
		}
	};

	@BeforeClass
	public static void setUpSuite() throws InterruptedException {
		Properties context = System.getProperties();
		queryUtils = ((QueryUtilsBuilder) context.get("queryUtilsBuilder")).build();
		cassandraUtils = (CassandraUtils) context.get("cassandraUtils");
		cassandraUtils.execute(queryUtils.createKeyspaceQuery(),
		                       queryUtils.createTableQuery(),
		                       queryUtils.createIndex(TestingConstants.INDEX_NAME_CONSTANT),
		                       queryUtils.getInsert(QueryTypeDataHelper.data1),
		                       queryUtils.getInsert(QueryTypeDataHelper.data2),
		                       queryUtils.getInsert(QueryTypeDataHelper.data3),
		                       queryUtils.getInsert(QueryTypeDataHelper.data4),
		                       queryUtils.getInsert(QueryTypeDataHelper.data5));
	}

	@AfterClass
	public static void tearDownSuite() {
		cassandraUtils.execute(queryUtils.dropIndexQuery(TestingConstants.INDEX_NAME_CONSTANT));
		cassandraUtils.execute(queryUtils.truncateTableQuery());
		cassandraUtils.execute(queryUtils.dropKeyspaceQuery());
	}
}
