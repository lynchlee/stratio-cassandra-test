package com.stratio.cassandra.lucene.story;

/**
 * Created by Jcalderin on 24/03/14.
 */

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.suite.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.DataHelper;
import com.stratio.cassandra.lucene.util.QueryUtils;

@RunWith(JUnit4.class)
public class SimpleDataHandlingTest {

    private static final Logger logger = Logger
            .getLogger(SimpleDataHandlingTest.class);

    private static QueryUtils queryUtils;

    private static CassandraUtils cassandraUtils;

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
        queriesList.add(queryUtils.getInsert(DataHelper.data1));
        queriesList.add(queryUtils.getInsert(DataHelper.data2));
        queriesList.add(queryUtils.getInsert(DataHelper.data3));

        cassandraUtils.executeQueriesList(queriesList);

        // Waiting for the custom index to be refreshed
        logger.debug("Waiting for the index to be refreshed...");
        Thread.sleep(1000);
        logger.debug("Index ready to rock!");
    }

    @AfterClass
    public static void tearDownTests() {
        // Dropping keyspace
        logger.debug("Dropping keyspace");
        cassandraUtils.executeQuery(queryUtils.dropKeyspaceQuery());
    }

    @Test()
    public void singleInsertion() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("ascii_1", "*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

}
