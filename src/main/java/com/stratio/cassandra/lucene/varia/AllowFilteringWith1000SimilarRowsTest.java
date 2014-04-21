package com.stratio.cassandra.lucene.varia;

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
import org.junit.AfterClass;
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
public class AllowFilteringWith1000SimilarRowsTest extends Abstract1000RegistersWatchedTest {

    private static final Logger logger = Logger
            .getLogger(AllowFilteringWith1000SimilarRowsTest.class);

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

        queryUtils = new QueryUtils(TestingConstants.KEYSPACE_CONSTANT,
                TestingConstants.TABLE_NAME_CONSTANT, columns, primaryKey,
                TestingConstants.INDEX_COLUMN_CONSTANT);

        cassandraUtils = new CassandraUtils(
                TestingConstants.CASSANDRA_LOCALHOST_CONSTANT);

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

        queriesList.addAll(VariaDataHelper.generateCustomInsertions(1000,
                queryUtils));

        cassandraUtils.executeQueriesList(queriesList, true);
    };

    @AfterClass
    public static void tearDownSuite() {

        // Dropping keyspace
        logger.debug("Dropping keyspace");
        cassandraUtils.executeQuery(queryUtils.dropKeyspaceQuery());
    }

    @Test
    public void allowFiltering1000rowsTest() {

        // Building up query
        String query = queryUtils.getWildcardQuery("ascii_1", "*", null);
        query = query.substring(0, query.length() - 1);
        query += " and double_1 = 1 allow filtering;";

        logger.debug("allow filtering query [" + query + "]");

        // Checking data
        ResultSet queryResult = cassandraUtils.executeQuery(query);

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1000 results!", 1000, rows.size());
    }

    @Test
    public void allowFilteringAndLimit999Test() {

        // Building up query
        String query = queryUtils.getWildcardQuery("ascii_1", "*", null);
        query = query.substring(0, query.length() - 1);
        query += " and double_1 = 1 limit 999 allow filtering";

        logger.debug("allow filtering query [" + query + "]");

        // Checking data
        ResultSet queryResult = cassandraUtils.executeQuery(query);

        List<Row> rows = queryResult.all();

        assertEquals("Expected 999 results!", 999, rows.size());
    }

    @Test
    public void allowFilteringAndLimit1001Test() {

        // Building up query
        String query = queryUtils.getWildcardQuery("ascii_1", "*", null);
        query = query.substring(0, query.length() - 1);
        query += " and double_1 = 1 limit 1001 allow filtering";

        logger.debug("allow filtering query [" + query + "]");

        // Checking data
        ResultSet queryResult = cassandraUtils.executeQuery(query);

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1000 results!", 1000, rows.size());
    }

    @Test
    public void allowFilteringAndLimit99Test() {

        // Building up query
        String query = queryUtils.getWildcardQuery("ascii_1", "*", null);
        query = query.substring(0, query.length() - 1);
        query += " and double_1 = 1 limit 99 allow filtering";

        logger.debug("allow filtering query [" + query + "]");

        // Checking data
        ResultSet queryResult = cassandraUtils.executeQuery(query);

        List<Row> rows = queryResult.all();

        assertEquals("Expected 99 results!", 99, rows.size());
    }

    @Test
    public void allowFilteringAndLimit101Test() {

        // Building up query
        String query = queryUtils.getWildcardQuery("ascii_1", "*", null);
        query = query.substring(0, query.length() - 1);
        query += " and double_1 = 1 limit 101 allow filtering";

        logger.debug("allow filtering query [" + query + "]");

        // Checking data
        ResultSet queryResult = cassandraUtils.executeQuery(query);

        List<Row> rows = queryResult.all();

        assertEquals("Expected 101 results!", 101, rows.size());
    }

    @Test
    public void allowFilteringAndLimit100Test() {

        // Building up query
        String query = queryUtils.getWildcardQuery("ascii_1", "*", null);
        query = query.substring(0, query.length() - 1);
        query += " and double_1 = 1 limit 100 allow filtering";

        logger.debug("allow filtering query [" + query + "]");

        // Checking data
        ResultSet queryResult = cassandraUtils.executeQuery(query);

        List<Row> rows = queryResult.all();

        assertEquals("Expected 100 results!", 100, rows.size());
    }
}