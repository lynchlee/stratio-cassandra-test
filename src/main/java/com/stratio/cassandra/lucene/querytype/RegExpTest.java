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

@RunWith(JUnit4.class)
public class RegExpTest extends AbstractWatchedTest {

    @Test
    public void regexpAsciiFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("ascii_1", "frase.*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void regexpAsciiFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("ascii_1", "frase .*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void regexpAsciiFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("ascii_1", ".*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void regexpAsciiFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("ascii_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void regexpAsciiFieldTest5() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("ascii_1", "frase tipo ascii", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void regexpInetFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("inet_1", ".*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void regexpInetFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("inet_1", "127.*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void regexpInetFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("inet_1", "127.1.*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void regexpInetFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("inet_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void regexpInetFieldTest5() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("inet_1", "127.1.1.1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    // FIXME TSocketException!
    public void regexpTextFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("text_1", ".*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void regexpTextFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("text_1", "Frase.*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void regexpTextFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("text_1", "Frase .*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    // FIXME TSocketException!
    public void regexpTextFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("text_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    // FIXME TSocketException!
    public void regexpTextFieldTest5() {

        ResultSet queryResult = cassandraUtils
                .executeQuery(queryUtils
                        .getRegexpQuery(
                                "text_1",
                                "Frase con espacios articulos y las palabras suficientes",
                                null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void regexpVarcharFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("varchar_1", ".*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void regexpVarcharFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("varchar_1", "frase.*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void regexpVarcharFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("varchar_1", "frase .*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 results!", 1, rows.size());
    }

    @Test
    public void regexpVarcharFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("varchar_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void regexpVarcharFieldTest5() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("varchar_1", "frasesencillasinespacios", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 results!", 1, rows.size());
    }
}
