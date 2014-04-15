package com.stratio.cassandra.lucene.querytype;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.QueryUtils;

public abstract class AbstractWatchedTest {

    private static final Logger logger = Logger
            .getLogger(AbstractWatchedTest.class);

    private static long startingTime;

    protected static QueryUtils queryUtils;

    protected static CassandraUtils cassandraUtils;

    @Rule
    public TestRule watcher = new TestWatcher() {

        protected void starting(Description description) {
            logger.info("*************************** Starting test ["
                    + description.getMethodName()
                    + "] ****************************");
            startingTime = System.currentTimeMillis();
        }

        protected void finished(Description description) {
            logger.info("------------------ Duration ["
                    + (System.currentTimeMillis() - startingTime)
                    + " ms] --------------------");
            logger.info("************************************************************************************************************");
        }
    };

    @BeforeClass
    public static void setUpTests() throws InterruptedException {

        Properties context = System.getProperties();
        queryUtils = (QueryUtils) context.get("queryUtils");
        cassandraUtils = (CassandraUtils) context.get("cassandraUtils");

        // Executing db queries
        List<String> queriesList = new ArrayList<>();

        String keyspaceCreationQuery = queryUtils
                .createKeyspaceQuery(TestingConstants.REPLICATION_FACTOR_2_CONSTANT);
        String tableCreationQuery = queryUtils.createTableQuery();
        String indexCreationQuery = queryUtils
                .createIndex(TestingConstants.INDEX_NAME_CONSTANT);

        queriesList.add(keyspaceCreationQuery);
        queriesList.add(tableCreationQuery);
        queriesList.add(indexCreationQuery);

        cassandraUtils.executeQueriesList(queriesList);
    }

    @AfterClass
    public static void tearDownTests() {

        cassandraUtils.executeQuery(queryUtils.dropKeyspaceQuery());
    }

    @Before
    public void setUpTest() {

        // Executing db queries
        List<String> queriesList = new ArrayList<>();

        queriesList.add(queryUtils.getInsert(QueryTypeDataHelper.data1));
        queriesList.add(queryUtils.getInsert(QueryTypeDataHelper.data2));
        queriesList.add(queryUtils.getInsert(QueryTypeDataHelper.data3));
        queriesList.add(queryUtils.getInsert(QueryTypeDataHelper.data4));

        cassandraUtils.executeQueriesList(queriesList, true);
    }

    @After
    public void tearDownTest() {

        cassandraUtils.executeQuery(queryUtils.truncateTableQuery(), true);
    }
}
