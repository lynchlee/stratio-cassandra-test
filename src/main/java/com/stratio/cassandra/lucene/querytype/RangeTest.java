package com.stratio.cassandra.lucene.querytype;

/**
 * Created by Jcalderin on 24/03/14.
 */

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import com.stratio.cassandra.lucene.util.QueryUtils;

@RunWith(JUnit4.class)
public class RangeTest extends AbstractWatchedTest {

    private static final Logger logger = Logger.getLogger(RangeTest.class);

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
        queriesList.add(queryUtils.getInsert(DataHelper.data4));

        cassandraUtils.executeQueriesList(queriesList, true);
    }

    @AfterClass
    public static void tearDownTests() {
        // Dropping keyspace
        logger.debug("Dropping keyspace");
        cassandraUtils.executeQuery(queryUtils.dropKeyspaceQuery());
    }

    @Test
    public void rangeAsciiFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("ascii_1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeAsciiFieldTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "g");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("ascii_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeAsciiFieldTest3() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "b");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("ascii_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeAsciiFieldTest4() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "f");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("ascii_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeAsciiFieldTest5() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "f");
        params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
        params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("ascii_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeIntegerTest1() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "-5");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "5");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("integer_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeIntegerTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "-4");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "4");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("integer_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 3 results!", 3, rows.size());
    }

    @Test
    public void rangeIntegerTest3() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "-4");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "-1");
        params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
        params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("integer_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeIntegerTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("integer_1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeBigintTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("bigint_1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeBigintTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "999999999999999");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "1000000000000001");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("bigint_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void rangeBigintTest3() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "1000000000000000");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "2000000000000000");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("bigint_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeBigintTest4() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "1000000000000000");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "2000000000000000");
        params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
        params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("bigint_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void rangeBigintTest5() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "1");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "3");
        params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
        params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("bigint_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeBlobTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("blob_1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeBlobTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "0x3E0A15");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "0x3E0A17");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("blob_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 3 results!", 3, rows.size());
    }

    @Test
    public void rangeBlobTest3() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "0x3E0A16");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "0x3E0A17");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("blob_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeBlobTest4() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "0x3E0A16");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "0x3E0A17");
        params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
        params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("blob_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 3 results!", 3, rows.size());
    }

    @Test
    public void rangeBlobTest5() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "0x3E0A17");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "0x3E0A18");
        params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
        params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("blob_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeBooleanTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("boolean_1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeDecimalTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("decimal_1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeDecimalTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "1999999999.9");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "2000000000.1");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("decimal_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void rangeDecimalTest3() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "2000000000.0");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "3000000000.0");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("decimal_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeDecimalTest4() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "2000000000.0");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "3000000000.0");
        params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
        params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("decimal_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 3 results!", 3, rows.size());
    }

    @Test
    public void rangeDecimalTest5() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "2000000000.000001");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "2000000000.181235");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("decimal_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    // @Test
    // public void rangeDateTest1() {
    //
    // ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
    // .getRangeQuery("date_1",
    // String.valueOf(System.currentTimeMillis()), null));
    //
    // List<Row> rows = queryResult.all();
    //
    // assertEquals("Expected 0 results!", 0, rows.size());
    // }
    //
    // @Test
    // public void rangeDateTest2() {
    //
    // ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
    // .getRangeQuery("date_1",
    // String.valueOf(System.currentTimeMillis() - 87000000),
    // null));
    //
    // List<Row> rows = queryResult.all();
    //
    // assertEquals("Expected 0 results!", 0, rows.size());
    // }
    //
    // @Test
    // public void rangeDateTest3() {
    //
    // ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
    // .getRangeQuery("date_1", "0", null));
    //
    // List<Row> rows = queryResult.all();
    //
    // assertEquals("Expected 0 results!", 0, rows.size());
    // }
    //
    // @Test
    // public void rangeDateTest4() {
    //
    // ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
    // .getRangeQuery("date_1",
    // String.valueOf(System.currentTimeMillis() + 87000000),
    // null));
    //
    // List<Row> rows = queryResult.all();
    //
    // assertEquals("Expected 0 results!", 0, rows.size());
    // }

    @Test
    public void rangeDoubleTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("double_1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeDoubleTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "1.9");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "2.1");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("double_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void rangeDoubleTest3() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "2.0");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "3.0");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("double_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeDoubleTest4() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "2.0");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "3.0");
        params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
        params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("double_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 3 results!", 3, rows.size());
    }

    @Test
    public void rangeDoubleTest5() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "7.0");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "10.0");
        params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
        params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("double_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeFloatTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("float_1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeFloatTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "1.9");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "2.1");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("float_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void rangeFloatTest3() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "1.0");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "2.0");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("float_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeFloatTest4() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "1.0");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "2.0");
        params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
        params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("float_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void rangeFloatTest5() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "7.0");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "9.0");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("float_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeUuidTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("uuid_1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeUuidTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "1");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "9");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("uuid_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeUuidTest3() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT,
                "60297440-b4fa-11e3-8b5a-0002a5d5c51c");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT,
                "60297440-b4fa-11e3-8b5a-0002a5d5c51d");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("uuid_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeUuidTest4() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT,
                "60297440-b4fa-11e3-8b5a-0002a5d5c51c");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT,
                "60297440-b4fa-11e3-8b5a-0002a5d5c51d");
        params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
        params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("uuid_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 3 results!", 3, rows.size());
    }

    @Test
    public void rangeTimeuuidTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("timeuuid_1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeTimeuuidTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "z");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("timeuuid_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeTimeuuidTest3() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT,
                "a4a70900-24e1-11df-8924-001ff3591712");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT,
                "a4a70900-24e1-11df-8924-001ff3591713");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("timeuuid_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeTimeuuidTest4() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT,
                "a4a70900-24e1-11df-8924-001ff3591712");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT,
                "a4a70900-24e1-11df-8924-001ff3591713");
        params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
        params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("timeuuid_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 3 results!", 3, rows.size());
    }

    @Test
    public void rangeInetFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("inet_1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeInetFieldTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "127.0.0.0");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "127.1.0.0");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("inet_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void rangeInetFieldTest3() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "127.0.0.0");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "127.1.0.0");
        params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
        params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("inet_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void rangeInetFieldTest4() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "192.168.0.0");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "192.168.0.1");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("inet_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeTextFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("text_1",

                null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeTextFieldTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "Frase");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "G");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("text_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeTextFieldTest3() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT,
                "Frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "G");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("text_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeTextFieldTest4() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT,
                "Frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "G");
        params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
        params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("text_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void rangeTextFieldTest5() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "G");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "H");
        params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
        params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("text_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeVarcharFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("varchar_1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeVarcharFieldTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "frase");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "g");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("varchar_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeVarcharFieldTest3() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT,
                "frasesencillasinespaciosperomaslarga");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "g");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("varchar_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeVarcharFieldTest4() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT,
                "frasesencillasinespaciosperomaslarga");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "g");
        params.put(TestingConstants.INCLUDE_LOWER_PARAM_CONSTANT, "true");
        params.put(TestingConstants.INCLUDE_UPPER_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("varchar_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void rangeVarcharFieldTest5() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "g");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "h");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("varchar_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }
}
