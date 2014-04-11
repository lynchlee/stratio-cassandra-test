package com.stratio.cassandra.lucene.querytype;

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
import com.stratio.cassandra.lucene.util.QueryUtils;

@RunWith(JUnit4.class)
public class PrefixTest extends AbstractWatchedTest {

    private static final Logger logger = Logger.getLogger(PrefixTest.class);

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
        queriesList.add(queryUtils.getInsert(DataHelper.data5));

        cassandraUtils.executeQueriesList(queriesList, true);
    }

    @AfterClass
    public static void tearDownTests() {
        // Dropping keyspace
        logger.debug("Dropping keyspace");
        cassandraUtils.executeQuery(queryUtils.dropKeyspaceQuery());
    }

    @Test()
    public void prefixAsciiFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("ascii_1", "frase ", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test()
    public void prefixAsciiFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("ascii_1", "frase", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test()
    public void prefixAsciiFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("ascii_1", "F", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test()
    public void prefixAsciiFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("ascii_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 5 results!", 5, rows.size());
    }

    @Test()
    public void prefixInetFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("inet_1", "127", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test()
    public void prefixInetFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("inet_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 5 results!", 5, rows.size());
    }

    @Test()
    public void prefixInetFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("inet_1", "127.0.", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test()
    public void prefixTextFieldTest1() {

        ResultSet queryResult = cassandraUtils
                .executeQuery(queryUtils
                        .getPrefixQuery(
                                "text_1",
                                "Frase con espacios articulos y las palabras suficientes",
                                null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test()
    public void prefixTextFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("text_1", "Frase", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test()
    public void prefixTextFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("text_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 5 results!", 5, rows.size());
    }

    @Test()
    public void prefixVarcharFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("varchar_1",
                        "frasesencillasinespaciosperomaslarga", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test()
    public void prefixVarcharFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("varchar_1", "frase", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test()
    public void prefixVarcharFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("varchar_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 5 results!", 5, rows.size());
    }
}
