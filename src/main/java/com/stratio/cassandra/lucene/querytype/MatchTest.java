package com.stratio.cassandra.lucene.querytype;

/**
 * Created by Jcalderin on 24/03/14.
 */

import static org.junit.Assert.assertEquals;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
public class MatchTest extends AbstractWatchedTest {

    private static final Logger logger = Logger.getLogger(MatchTest.class);

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

    @Test
    public void matchAsciiFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("ascii_1", "frase tipo ascii", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void matchAsciiFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("ascii_1", "frase", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchAsciiFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("ascii_1", "frase tipo asciii", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchAsciiFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("ascii_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchAsciiFieldTest5() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("ascii_1", "frase tipo asci", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchBigintTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("bigint_1", "1000000000000000", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void matchBigintTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("bigint_1", "3000000000000000", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void matchBigintTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("bigint_1", "10000000000000000", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchBigintTest5() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("bigint_1", "100000000000000", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchBlobTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("blob_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchBlobTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("blob_1", "3E0A16", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 3 results!", 3, rows.size());
    }

    @Test
    public void matchBlobTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("blob_1", "3E0A161", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchBlobTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("blob_1", "3E0A1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchBlobTest5() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("blob_1", "3E0A15", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void matchBooleanTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("boolean_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchBooleanTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("boolean_1", "true", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 3 results!", 3, rows.size());
    }

    @Test
    public void matchBooleanTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("boolean_1", "false", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void matchBooleanTest5() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("boolean_1", "else", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchDecimalTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("decimal_1", "3000000000.0", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void matchDecimalTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("decimal_1", "300000000.0", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchDecimalTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("decimal_1", "3000000000.00", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void matchDecimalTest5() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("decimal_1", "1000000000.0", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void matchDateTest1() {

        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Date date = new Date();

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("date_1",
                        String.valueOf(System.currentTimeMillis()), null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchDateTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("date_1",
                        String.valueOf(System.currentTimeMillis() - 87000000),
                        null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchDateTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("date_1", "0", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchDateTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("date_1",
                        String.valueOf(System.currentTimeMillis() + 87000000),
                        null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchDoubleTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("double_1", "0", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchDoubleTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("double_1", "2.0", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void matchDoubleTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("double_1", "2", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void matchDoubleTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("double_1", "2.00", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void matchFloatTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("float_1", "0", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchFloatTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("float_1", "2.0", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void matchFloatTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("float_1", "2", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void matchFloatTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("float_1", "2.00", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void matchIntegerTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("integer_1", "-2", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void matchIntegerTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("integer_1", "2", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchIntegerTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("integer_1", "0", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchIntegerTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("integer_1", "-1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void matchUuidTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("uuid_1", "0", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchUuidTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("uuid_1",
                        "60297440-b4fa-11e3-8b5a-0002a5d5c51b", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void matchUuidTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("uuid_1", "60297440-b4fa-11e3-0002a5d5c51b",
                        null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchTimeuuidTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("timeuuid_1", "0", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchTimeuuidTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("timeuuid_1",
                        "a4a70900-24e1-11df-8924-001ff3591711", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void matchTimeuuidTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("timeuuid_1", "a4a70900-24e1-11df-001ff3591711",
                        null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchInetFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("inet_1", "127.1.1.1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void matchInetFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("inet_1", "127.0.1.1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void matchInetFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("inet_1", "127.0.1.1.", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchInetFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("inet_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchTextFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("text_1", "Frase", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void matchTextFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("text_1", "Frase*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchTextFieldTest3() {

        ResultSet queryResult = cassandraUtils
                .executeQuery(queryUtils
                        .getMatchQuery(
                                "text_1",
                                "Frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga",
                                null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    @Ignore
    // TODO Remove ignore when "" doesn't fail
    public void matchTextFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("text_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchVarcharFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("varchar_1",
                        "frasesencillasinespaciosperomaslarga", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void matchVarcharFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("varchar_1", "frase*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchVarcharFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("varchar_1", "frasesencillasinespacios", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void matchVarcharFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("varchar_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }
}
