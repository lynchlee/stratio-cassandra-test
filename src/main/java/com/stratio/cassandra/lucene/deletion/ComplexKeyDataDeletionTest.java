package com.stratio.cassandra.lucene.deletion;

/**
 * Created by Jcalderin on 24/03/14.
 */

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.QueryUtils;

@RunWith(JUnit4.class)
public class ComplexKeyDataDeletionTest {

    private static final Logger logger = Logger
            .getLogger(ComplexKeyDataDeletionTest.class);

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
        String[] inarray = { "integer_1", "ascii_1" };
        String[] outarray = { "double_1" };
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

        String keyspaceCreationQuery = queryUtils
                .createKeyspaceQuery();
        String tableCreationQuery = queryUtils.createTableQuery();
        String indexCreationQuery = queryUtils
                .createIndex(TestingConstants.INDEX_NAME_CONSTANT);

        queriesList.add(keyspaceCreationQuery);
        queriesList.add(tableCreationQuery);
        queriesList.add(indexCreationQuery);
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data1));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data2));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data3));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data4));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data5));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data6));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data7));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data8));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data9));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data10));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data11));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data12));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data13));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data14));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data15));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data16));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data17));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data18));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data19));
        queriesList.add(queryUtils.getInsert(DeletionDataHelper.data20));

        cassandraUtils.executeQueriesList(queriesList, true);
    }

    @After
    public void tearDown() {
        // Dropping keyspace
        logger.debug("Dropping keyspace");
        cassandraUtils.executeQuery(queryUtils.dropKeyspaceQuery());
    }

    @Test
    public void columnDeletion() {

        cassandraUtils
                .executeQuery(
                        queryUtils
                                .constructValueDeleteQueryByCondition(
                                        "bigint_1",
                                        "integer_1 = 1 and ascii_1 = 'ascii' and double_1 = 1"),
                        true);

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("ascii_1", "*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 20 results!", 20, rows.size());

        int integerValue;
        String asciiValue;
        double doubleValue;
        for (Row row : rows) {
            integerValue = row.getInt("integer_1");
            asciiValue = row.getString("ascii_1");
            doubleValue = row.getDouble("double_1");
            if ((integerValue == 1) && (asciiValue.equals("ascii"))
                    && (doubleValue == 1)) {
                assertTrue("Must be null!", row.isNull("bigint_1"));
            }
        }
    }

    @Test
    public void mapElementDeletion() {

        cassandraUtils
                .executeQuery(
                        queryUtils
                                .constructValueDeleteQueryByCondition(
                                        "map_1['k1']",
                                        "integer_1 = 1 and ascii_1 = 'ascii' and double_1 = 1"),
                        true);

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("ascii_1", "*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 20 results!", 20, rows.size());

        int integerValue;
        String asciiValue;
        double doubleValue;
        Map<String, String> mapValue = null;
        for (Row row : rows) {
            integerValue = row.getInt("integer_1");
            asciiValue = row.getString("ascii_1");
            doubleValue = row.getDouble("double_1");
            if ((integerValue == 1) && (asciiValue.equals("ascii"))
                    && (doubleValue == 1)) {
                mapValue = row.getMap("map_1", String.class, String.class);
            }
        }

        assertNotNull("Must not be null!", mapValue);
        assertNull("Must be null!", mapValue.get("k1"));
    }

    @Test
    public void listElementDeletion() {

        cassandraUtils
                .executeQuery(
                        queryUtils
                                .constructValueDeleteQueryByCondition(
                                        "list_1[0]",
                                        "integer_1 = 1 and ascii_1 = 'ascii' and double_1 = 1"),
                        true);

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("ascii_1", "*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 20 results!", 20, rows.size());

        int integerValue;
        String asciiValue;
        double doubleValue;
        List<String> listValue = null;
        for (Row row : rows) {
            integerValue = row.getInt("integer_1");
            asciiValue = row.getString("ascii_1");
            doubleValue = row.getDouble("double_1");
            if ((integerValue == 1) && (asciiValue.equals("ascii"))
                    && (doubleValue == 1)) {
                listValue = row.getList("list_1", String.class);
            }
        }

        assertNotNull("Must not be null!", listValue);
        assertEquals("Lenght unexpected", 1, listValue.size());
    }

    @Test
    public void totalPartitionDeletion() {

        cassandraUtils
                .executeQuery(
                        queryUtils
                                .constructDeleteQueryByCondition("integer_1 = 1 and ascii_1 = 'ascii' and double_1 = 1"),
                        true);

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("ascii_1", "*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 19 results!", 19, rows.size());

    }

    @Test
    public void partialPartitionDeletion() {

        cassandraUtils
                .executeQuery(
                        queryUtils
                                .constructDeleteQueryByCondition("integer_1 = 1 and ascii_1 = 'ascii'"),
                        true);

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("ascii_1", "*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 18 results!", 18, rows.size());
    }
}
