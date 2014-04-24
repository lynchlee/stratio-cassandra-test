package com.stratio.cassandra.lucene.querytype;

/**
 * Created by Jcalderin on 24/03/14.
 */

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;

@RunWith(JUnit4.class)
public class WildcardTest extends AbstractWatchedTest {

    @Test
    public void wildcardAsciiFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("ascii_1", "*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void wildcardAsciiFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("ascii_1", "frase*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void wildcardAsciiFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("ascii_1", "frase *", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test(expected = InvalidQueryException.class)
    public void wildcardAsciiFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("ascii_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void wildcardInetFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("inet_1", "*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void wildcardInetFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("inet_1", "127*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void wildcardInetFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("inet_1", "127.1.*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test(expected = InvalidQueryException.class)
    public void wildcardInetFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("inet_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void wildcardTextFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("text_1", "*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void wildcardTextFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("text_1", "Frase*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    // FIXME Returns 0 and I'm expecting 3...
    public void wildcardTextFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("text_1", "Frasesin*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test(expected = InvalidQueryException.class)
    public void wildcardTextFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("text_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void wildcardVarcharFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("varchar_1", "*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void wildcardVarcharFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("varchar_1", "frase*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void wildcardVarcharFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("varchar_1", "frase sencilla*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 results!", 1, rows.size());
    }

    @Test(expected = InvalidQueryException.class)
    public void wildcardVarcharFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("varchar_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }
}
