package com.stratio.cassandra.lucene.varia;

/**
 * Created by Jcalderin on 24/03/14.
 */

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
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
import com.stratio.cassandra.lucene.querytype.BooleanSubqueryType;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.QueryUtils;

@RunWith(JUnit4.class)
public class FilterTest {

    private static final Logger logger = Logger.getLogger(FilterTest.class);

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
                .createKeyspaceQuery();
        String tableCreationQuery = queryUtils.createTableQuery();
        String indexCreationQuery = queryUtils
                .createIndex(TestingConstants.INDEX_NAME_CONSTANT);

        queriesList.add(keyspaceCreationQuery);
        queriesList.add(tableCreationQuery);
        queriesList.add(indexCreationQuery);

        queriesList.add(queryUtils.getInsert(FilterDataHelper.data1));
        queriesList.add(queryUtils.getInsert(FilterDataHelper.data2));
        queriesList.add(queryUtils.getInsert(FilterDataHelper.data3));
        queriesList.add(queryUtils.getInsert(FilterDataHelper.data4));
        queriesList.add(queryUtils.getInsert(FilterDataHelper.data5));
        queriesList.add(queryUtils.getInsert(FilterDataHelper.data6));
        queriesList.add(queryUtils.getInsert(FilterDataHelper.data7));

        cassandraUtils.executeQueriesList(queriesList, true);
    };

    @AfterClass
    public static void tearDownSuite() {

        // Dropping keyspace
        logger.debug("Dropping keyspace");
        cassandraUtils.executeQuery(queryUtils.dropKeyspaceQuery());
    }

    @Test
    public void booleanMustWithBoostTest() {

        Map<String, String> query1 = new LinkedHashMap<>();
        query1.put("type", "match");
        query1.put("value", "frase");
        query1.put("field", "text_1");
        query1.put("boost", "0.9");
        Map<String, String> query2 = new LinkedHashMap<>();
        query2.put("type", "fuzzy");
        query2.put("value", "127.1.0.1");
        query2.put("field", "inet_1");
        query2.put("boost", "0.1");

        List<Map<String, String>> subqueries = new LinkedList<>();
        subqueries.add(query1);
        subqueries.add(query2);

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getBooleanFilter(BooleanSubqueryType.MUST, subqueries, null));

        List<Row> firstRows = queryResult.all();

        assertEquals("Expected 3 results!", 3, firstRows.size());

        // Modifying boost values
        query1.put("boost", "0.1");
        query2.put("boost", "0.9");

        queryResult = cassandraUtils.executeQuery(queryUtils.getBooleanFilter(
                BooleanSubqueryType.MUST, subqueries, null));

        List<Row> secondRows = queryResult.all();

        assertEquals("Expected 3 results!", 3, secondRows.size());

        Row firstSetFirstRow = firstRows.get(0);
        Row secondSetFirstRow = secondRows.get(0);

        assertEquals("Expected same values!",
                firstSetFirstRow.getInt("integer_1"),
                secondSetFirstRow.getInt("integer_1"));
    }

}
