package com.stratio.cassandra.lucene.story;

/**
 * Created by Jcalderin on 24/03/14.
 */

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.suite.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.IndexHandlingUtils;
import com.stratio.cassandra.lucene.util.QueryUtils;

@RunWith(JUnit4.class)
public class SimpleDataHandlingTest {

    private static final Logger logger = Logger
            .getLogger(SimpleDataHandlingTest.class);

    private static QueryUtils queryUtils;

    private static CassandraUtils cassandraUtils;

    @Before
    public void setUp() throws InterruptedException {

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

        cassandraUtils.executeQueriesList(queriesList, true);
    }

    @After
    public void tearDown() {
        // Dropping keyspace
        logger.debug("Dropping keyspace");
        cassandraUtils.executeQuery(queryUtils.dropKeyspaceQuery());
    }

    @Test()
    public void singleInsertion() {

        // Data4 insertion
        cassandraUtils.executeQuery(queryUtils.getInsert(DataHelper.data4),
                true);

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("ascii_1", "*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());

        // Data5 insertion
        cassandraUtils.executeQuery(queryUtils.getInsert(DataHelper.data5),
                true);

        queryResult = cassandraUtils.executeQuery(queryUtils.getWildcardQuery(
                "ascii_1", "*", null));

        rows = queryResult.all();

        assertEquals("Expected 5 results!", 5, rows.size());

        // Data4 removal
        cassandraUtils.executeQuery(
                queryUtils.constructDeleteQueryByCondition("integer_1 = 4"),
                true);

        queryResult = cassandraUtils.executeQuery(queryUtils.getWildcardQuery(
                "ascii_1", "*", null));

        rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
        assertFalse("Element not expected!",
                IndexHandlingUtils.containsElementByIntegerKey(rows, 4));

        // Data5 removal
        cassandraUtils.executeQuery(
                queryUtils.constructDeleteQueryByCondition("integer_1 = 5"),
                true);

        queryResult = cassandraUtils.executeQuery(queryUtils.getWildcardQuery(
                "ascii_1", "*", null));

        rows = queryResult.all();

        assertEquals("Expected 3 results!", 3, rows.size());
        assertFalse("Element not expected!",
                IndexHandlingUtils.containsElementByIntegerKey(rows, 5));

        // Data2 removal
        cassandraUtils.executeQuery(
                queryUtils.constructDeleteQueryByCondition("integer_1 = 2"),
                true);

        queryResult = cassandraUtils.executeQuery(queryUtils.getWildcardQuery(
                "ascii_1", "*", null));

        rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
        assertFalse("Element not expected!",
                IndexHandlingUtils.containsElementByIntegerKey(rows, 2));

        // Data3 removal
        cassandraUtils.executeQuery(
                queryUtils.constructDeleteQueryByCondition("integer_1 = 3"),
                true);

        queryResult = cassandraUtils.executeQuery(queryUtils.getWildcardQuery(
                "ascii_1", "*", null));

        rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
        assertFalse("Element not expected!",
                IndexHandlingUtils.containsElementByIntegerKey(rows, 3));

        // Data1 removal
        cassandraUtils.executeQuery(
                queryUtils.constructDeleteQueryByCondition("integer_1 = 1"),
                true);

        queryResult = cassandraUtils.executeQuery(queryUtils.getWildcardQuery(
                "ascii_1", "*", null));

        rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
        assertFalse("Element not expected!",
                IndexHandlingUtils.containsElementByIntegerKey(rows, 1));
    }

    @Test()
    public void multipleInsertion() {

        // Data4 and data5 insertion
        List<String> queriesList = new ArrayList<>();
        queriesList.add(queryUtils.getInsert(DataHelper.data4));
        queriesList.add(queryUtils.getInsert(DataHelper.data5));

        cassandraUtils.executeQueriesList(queriesList, true);

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("ascii_1", "*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 5 results!", 5, rows.size());

        // Data4 removal
        cassandraUtils.executeQuery(
                queryUtils.constructDeleteQueryByCondition("integer_1 = 4"),
                true);

        queryResult = cassandraUtils.executeQuery(queryUtils.getWildcardQuery(
                "ascii_1", "*", null));

        rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
        assertFalse("Element not expected!",
                IndexHandlingUtils.containsElementByIntegerKey(rows, 4));

        // Data5 removal
        cassandraUtils.executeQuery(
                queryUtils.constructDeleteQueryByCondition("integer_1 = 5"),
                true);

        queryResult = cassandraUtils.executeQuery(queryUtils.getWildcardQuery(
                "ascii_1", "*", null));

        rows = queryResult.all();

        assertEquals("Expected 3 results!", 3, rows.size());
        assertFalse("Element not expected!",
                IndexHandlingUtils.containsElementByIntegerKey(rows, 5));

        // Data2 removal
        cassandraUtils.executeQuery(
                queryUtils.constructDeleteQueryByCondition("integer_1 = 2"),
                true);

        queryResult = cassandraUtils.executeQuery(queryUtils.getWildcardQuery(
                "ascii_1", "*", null));

        rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
        assertFalse("Element not expected!",
                IndexHandlingUtils.containsElementByIntegerKey(rows, 2));

        // Data3 removal
        cassandraUtils.executeQuery(
                queryUtils.constructDeleteQueryByCondition("integer_1 = 3"),
                true);

        queryResult = cassandraUtils.executeQuery(queryUtils.getWildcardQuery(
                "ascii_1", "*", null));

        rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
        assertFalse("Element not expected!",
                IndexHandlingUtils.containsElementByIntegerKey(rows, 3));

        // Data1 removal
        cassandraUtils.executeQuery(
                queryUtils.constructDeleteQueryByCondition("integer_1 = 1"),
                true);

        queryResult = cassandraUtils.executeQuery(queryUtils.getWildcardQuery(
                "ascii_1", "*", null));

        rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
        assertFalse("Element not expected!",
                IndexHandlingUtils.containsElementByIntegerKey(rows, 1));
    }

    @Test()
    public void multipleDeletion() {

        // Data2 & data3 removal
        List<String> queriesList = new ArrayList<>();
        queriesList.add(queryUtils
                .constructDeleteQueryByCondition("integer_1 = 2"));
        queriesList.add(queryUtils
                .constructDeleteQueryByCondition("integer_1 = 3"));
        cassandraUtils.executeQueriesList(queriesList, true);

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("ascii_1", "*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
        assertFalse("Element not expected!",
                IndexHandlingUtils.containsElementByIntegerKey(rows, 3));

        // Data1 removal
        cassandraUtils.executeQuery(
                queryUtils.constructDeleteQueryByCondition("integer_1 = 1"),
                true);

        queryResult = cassandraUtils.executeQuery(queryUtils.getWildcardQuery(
                "ascii_1", "*", null));

        rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
        assertFalse("Element not expected!",
                IndexHandlingUtils.containsElementByIntegerKey(rows, 1));
    }
}
