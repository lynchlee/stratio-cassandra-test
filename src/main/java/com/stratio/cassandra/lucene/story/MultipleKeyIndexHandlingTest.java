package com.stratio.cassandra.lucene.story;

/**
 * Created by Jcalderin on 24/03/14.
 */

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.QueryUtils;

@RunWith(JUnit4.class)
public class MultipleKeyIndexHandlingTest {

    private static final Logger logger = Logger
            .getLogger(MultipleKeyIndexHandlingTest.class);

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
        String[] outarray = { "ascii_1" };
        List<String> in = Arrays.asList(inarray);
        List<String> out = Arrays.asList(outarray);
        primaryKey.put("in", in);
        primaryKey.put("out", out);

        queryUtils = new QueryUtils(TestingConstants.KEYSPACE_CONSTANT,
                TestingConstants.TABLE_NAME_CONSTANT, columns, primaryKey,
                TestingConstants.INDEX_COLUMN_CONSTANT);

        cassandraUtils = new CassandraUtils(
                TestingConstants.CASSANDRA_LOCALHOST_CONSTANT);
    };

    @AfterClass
    public static void tearDownSuite() {

        cassandraUtils.disconnect();
    };

    @Before
    public void setUp() throws InterruptedException {

        // Executing db queries
        List<String> queriesList = new ArrayList<>();

        String keyspaceCreationQuery = queryUtils.createKeyspaceQuery();
        String tableCreationQuery = queryUtils.createTableQuery();

        queriesList.add(keyspaceCreationQuery);
        queriesList.add(tableCreationQuery);

        cassandraUtils.executeQueriesList(queriesList);
    }

    @After
    public void tearDown() {

        // Dropping keyspace
        logger.debug("Dropping keyspace");
        cassandraUtils.executeQuery(queryUtils.dropKeyspaceQuery());
    }

    @Test
    @Ignore("Waiting for Cassandra to fix the keyspace recreation bug")
    public void createIndexAfterInsertionsTest() {

        List<String> queriesList = new ArrayList<>();

        // Preparing data
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data1));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data2));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data3));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data4));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data5));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data6));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data7));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data8));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data9));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data10));

        // Creating index
        String indexCreationQuery = queryUtils
                .createIndex(TestingConstants.INDEX_NAME_CONSTANT);
        queriesList.add(indexCreationQuery);

        cassandraUtils.executeQueriesList(queriesList, true);

        // Checking data
        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("ascii_1", "*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 10 results!", 10, rows.size());
    }

    @Test
    @Ignore("Waiting for Cassandra to fix the keyspace recreation bug")
    public void createIndexDuringInsertionsTest1() {

        List<String> queriesList = new ArrayList<>();

        // Preparing initial data
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data1));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data2));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data3));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data4));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data5));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data6));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data7));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data8));

        // Creating index
        String indexCreationQuery = queryUtils
                .createIndex(TestingConstants.INDEX_NAME_CONSTANT);
        queriesList.add(indexCreationQuery);

        // Inserting last data
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data9));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data10));

        cassandraUtils.executeQueriesList(queriesList, true);

        // Checking data
        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("ascii_1", "*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 10 results!", 10, rows.size());
    }

    @Test
    @Ignore("Waiting for Cassandra to fix the keyspace recreation bug")
    public void createIndexDuringInsertionsTest2() {

        List<String> queriesList = new ArrayList<>();

        // Preparing initial data
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data1));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data2));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data3));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data4));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data6));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data7));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data8));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data9));

        // Creating index
        String indexCreationQuery = queryUtils
                .createIndex(TestingConstants.INDEX_NAME_CONSTANT);
        queriesList.add(indexCreationQuery);

        // Inserting last data
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data5));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data10));

        cassandraUtils.executeQueriesList(queriesList, true);

        // Checking data
        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("ascii_1", "*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 10 results!", 10, rows.size());
    }

    @Test
    @Ignore("Waiting for Cassandra to fix the keyspace recreation bug")
    public void createIndexDuringInsertionsTest3() {

        List<String> queriesList = new ArrayList<>();

        // Preparing initial data
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data2));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data3));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data4));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data5));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data6));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data7));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data8));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data9));

        // Creating index
        String indexCreationQuery = queryUtils
                .createIndex(TestingConstants.INDEX_NAME_CONSTANT);
        queriesList.add(indexCreationQuery);

        // Inserting last data
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data1));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data10));

        cassandraUtils.executeQueriesList(queriesList, true);

        // Checking data
        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("ascii_1", "*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 10 results!", 10, rows.size());
    }

    @Test
    @Ignore("Waiting for Cassandra to fix the keyspace recreation bug")
    public void recreateIndexAfterInsertionsTest() {

        List<String> queriesList = new ArrayList<>();

        // Creating index
        String indexCreationQuery = queryUtils
                .createIndex(TestingConstants.INDEX_NAME_CONSTANT);
        queriesList.add(indexCreationQuery);

        // Preparing initial data
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data1));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data2));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data3));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data4));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data5));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data6));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data7));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data8));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data9));
        queriesList.add(queryUtils.getInsert(StoryDataHelper.data10));

        cassandraUtils.executeQueriesList(queriesList, true);

        // Checking data
        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("ascii_1", "*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 10 results!", 10, rows.size());

        // Dropping index
        cassandraUtils
                .executeQuery(queryUtils
                        .dropIndexQuery(TestingConstants.INDEX_NAME_CONSTANT),
                        true);

        // Recreating index
        cassandraUtils.executeQuery(indexCreationQuery, true);

        // Checking data
        queryResult = cassandraUtils.executeQuery(queryUtils.getWildcardQuery(
                "ascii_1", "*", null));

        rows = queryResult.all();

        assertEquals("Expected 10 results!", 10, rows.size());
    }
}