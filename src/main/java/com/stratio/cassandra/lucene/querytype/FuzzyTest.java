package com.stratio.cassandra.lucene.querytype;

/**
 * Created by Jcalderin on 24/03/14.
 */

import static org.junit.Assert.assertEquals;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.TestingConstants;

@RunWith(JUnit4.class)
public class FuzzyTest extends AbstractWatchedTest {

    @Test
    public void fuzzyAsciiFieldTest() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("ascii_1", "frase tipo asci", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void emptyFuzzyAsciiFieldTest() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("ascii_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void fuzzyAsciiFieldWith1MaxEditsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "1");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("ascii_1", "frase tipo asci", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void fuzzyAsciiFieldWith0MaxEditsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "0");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("ascii_1", "frase tipo asci", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void fuzzyAsciiFieldWith2PrefixLengthTest1() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.PREFIX_LENGTH_PARAM_CONSTANT, "2");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("ascii_1", "frase typo ascii", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void fuzzyAsciiFieldWith2PrefixLengthTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.PREFIX_LENGTH_PARAM_CONSTANT, "2");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("ascii_1", "phrase tipo ascii", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void fuzzyAsciiFieldWith1MaxExpansionsTest()
            throws InterruptedException {

        // Adding new data for the test
        cassandraUtils.executeQuery(
                queryUtils.getInsert(QueryTypeDataHelper.data5), true);

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EXPANSIONS_PARAM_CONSTANT, "1");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("ascii_1", "frase tipo ascii", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void fuzzyAsciiFieldWith10MaxExpansionsTest()
            throws InterruptedException {

        // Adding new data for the test
        cassandraUtils.executeQuery(
                queryUtils.getInsert(QueryTypeDataHelper.data5), true);

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EXPANSIONS_PARAM_CONSTANT, "10");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("ascii_1", "frase tipo ascii", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void fuzzyAsciiFieldWithoutTranspositionsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "false");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("ascii_1", "farse itpo ascii", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void fuzzyAsciiFieldWithTranspositionsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("ascii_1", "farse itpo ascii", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void fuzzyAsciiFieldWith5MaxEditsAndTranspositionsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "1");
        params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("ascii_1", "farse itpo ascii", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());

    }

    @Test
    public void fuzzyInetFieldTest() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("inet_1", "127.0.1.1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void emptyFuzzyInetFieldTest() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("inet_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void fuzzyInetFieldWith1MaxEditsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "1");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("inet_1", "127.0.0.1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void fuzzyInetFieldWith0MaxEditsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "0");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("inet_1", "127.0.1.1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void fuzzyInetFieldWith2PrefixLengthTest1() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.PREFIX_LENGTH_PARAM_CONSTANT, "2");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("inet_1", "127.0.1.1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void fuzzyInetFieldWith2PrefixLengthTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.PREFIX_LENGTH_PARAM_CONSTANT, "2");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("inet_1", "117.0.1.1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void fuzzyInetFieldWith1MaxExpansionsTest()
            throws InterruptedException {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EXPANSIONS_PARAM_CONSTANT, "1");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("inet_1", "127.0.1.1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void fuzzyInetFieldWith10MaxExpansionsTest()
            throws InterruptedException {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EXPANSIONS_PARAM_CONSTANT, "10");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("inet_1", "127.0.1.1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void fuzzyInetFieldWithoutTranspositionsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "false");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("inet_1", "1270..1.1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 3 results!", 3, rows.size());
    }

    @Test
    public void fuzzyInetFieldWithTranspositionsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("inet_1", "1270..1.1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void fuzzyInetFieldWith1MaxEditsAndTranspositionsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "1");
        params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("inet_1", "1270..1.1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void fuzzyTextFieldTest() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("text_1",
                        "Frasesinespaciosconarticulosylaspalabrassuficiente",
                        null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    // FIXME TSocketException!
    public void emptyFuzzyTextFieldTest() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("text_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void fuzzyTextFieldWith1MaxEditsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "1");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("text_1",
                        "Frasesinespaciosconarticulosylaspalabrassuficiente",
                        params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void fuzzyTextFieldWith0MaxEditsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "0");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("text_1",
                        "Frasesinespaciosconarticulosylaspalabrassuficiente",
                        params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void fuzzyTextFieldWith2PrefixLengthTest1() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.PREFIX_LENGTH_PARAM_CONSTANT, "2");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("text_1",
                        "Frasesinespaciosconarticulosylaspalabrassuficiente",
                        params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void fuzzyTextFieldWith2PrefixLengthTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.PREFIX_LENGTH_PARAM_CONSTANT, "2");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("text_1",
                        "rFasesinespaciosconarticulosylaspalabrassuficiente",
                        params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void fuzzyTextFieldWith1MaxExpansionsTest()
            throws InterruptedException {

        // Adding new data for the test
        cassandraUtils.executeQuery(
                queryUtils.getInsert(QueryTypeDataHelper.data5), true);

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EXPANSIONS_PARAM_CONSTANT, "1");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("text_1",
                        "Frasesinespaciosconarticulosylaspalabrassuficiente",
                        params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void fuzzyTextFieldWith10MaxExpansionsTest()
            throws InterruptedException {

        // Adding new data for the test
        cassandraUtils.executeQuery(
                queryUtils.getInsert(QueryTypeDataHelper.data5), true);

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EXPANSIONS_PARAM_CONSTANT, "10");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("text_1",
                        "Frasesinespaciosconarticulosylaspalabrassuficiente",
                        params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void fuzzyTextFieldWithoutTranspositionsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "false");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("text_1",
                        "Frasseinespacisoconarticulosylaspalabrassuficientes",
                        params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void fuzzyTextFieldWithTranspositionsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("text_1",
                        "Frasseinespacisoconarticulosylaspalabrassuficientes",
                        params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test
    public void fuzzyTextFieldWith5MaxEditsAndTranspositionsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "1");
        params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("text_1",
                        "Frasseinespacisoconarticulosylaspalabrassuficientes",
                        params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void fuzzyVarcharFieldTest() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("varchar_1",
                        "frasesencillasnespaciosperomaslarga", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void emptyFuzzyVarcharFieldTest() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("varchar_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void fuzzyVarcharFieldWith1MaxEditsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "1");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("varchar_1",
                        "frasesencillasnespaciosperomaslarga", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void fuzzyVarcharFieldWith0MaxEditsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "0");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("varchar_1",
                        "frasesencillasnespaciosperomaslarga", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void fuzzyVarcharFieldWith2PrefixLengthTest1() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.PREFIX_LENGTH_PARAM_CONSTANT, "2");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("varchar_1",
                        "frasesencillasnespaciosperomaslarga", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 result2!", 2, rows.size());
    }

    @Test
    public void fuzzyVarcharFieldWith2PrefixLengthTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.PREFIX_LENGTH_PARAM_CONSTANT, "2");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("varchar_1",
                        "rfasesencillasnespaciosperomaslarga", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void fuzzyVarcharFieldWith1MaxExpansionsTest()
            throws InterruptedException {

        // Adding new data for the test
        cassandraUtils.executeQuery(
                queryUtils.getInsert(QueryTypeDataHelper.data5), true);

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EXPANSIONS_PARAM_CONSTANT, "1");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("varchar_1",
                        "frasesencillasnespaciosperomaslarga", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    // FIXME Returns 4 results: row -5 twice!
    public void fuzzyVarcharFieldWith10MaxExpansionsTest()
            throws InterruptedException {

        // Adding new data for the test
        cassandraUtils.executeQuery(
                queryUtils.getInsert(QueryTypeDataHelper.data5), true);

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EXPANSIONS_PARAM_CONSTANT, "10");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("varchar_1",
                        "frasesencillasnespaciosperomaslarga", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 3 results!", 3, rows.size());
    }

    @Test
    public void fuzzyVarcharFieldWithoutTranspositionsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "false");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("varchar_1",
                        "frasesenicllasnespaciosperomaslarga", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void fuzzyVarcharFieldWithTranspositionsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("varchar_1",
                        "frasesenicllasnespaciosperomaslarga", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void fuzzyVarcharFieldWith5MaxEditsAndTranspositionsTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.MAX_EDITS_PARAM_CONSTANT, "1");
        params.put(TestingConstants.TRANSPOSITIONS_PARAM_CONSTANT, "true");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("varchar_1",
                        "frasesenicllasnespaciosperomaslarga", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }
}
