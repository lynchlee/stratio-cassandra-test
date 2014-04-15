package com.stratio.cassandra.lucene.querytype;

/**
 * Created by Jcalderin on 24/03/14.
 */

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

@RunWith(JUnit4.class)
public class PhraseTest extends AbstractWatchedTest {

    @Override
    @Before
    public void setUpTest() {

        // Executing db queries
        List<String> queriesList = new ArrayList<>();

        queriesList.add(queryUtils.getInsert(QueryTypeDataHelper.data1));
        queriesList.add(queryUtils.getInsert(QueryTypeDataHelper.data2));
        queriesList.add(queryUtils.getInsert(QueryTypeDataHelper.data3));
        queriesList.add(queryUtils.getInsert(QueryTypeDataHelper.data4));
        queriesList.add(queryUtils.getInsert(QueryTypeDataHelper.data5));

        cassandraUtils.executeQueriesList(queriesList, true);
    }

    @Test()
    public void phraseTextFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPhraseQuery("text_1", Arrays.asList("Frase", "espacios"),
                        null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test()
    public void phraseTextFieldWithSlopTest1() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put("slop", "2");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPhraseQuery("text_1", Arrays.asList("Frase", "espacios"),
                        params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test()
    public void phraseTextFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPhraseQuery("text_1",
                        Arrays.asList("articulos", "suficientes"), null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test()
    public void phraseTextFieldWithSlopTest() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put("slop", "2");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPhraseQuery("text_1",
                        Arrays.asList("articulos", "palabras"), params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 1 result!", 1, rows.size());
    }

    @Test()
    public void phraseTextFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPhraseQuery("text_1", Arrays.asList("con", "los"), null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test()
    public void phraseTextFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPhraseQuery("text_1", null, null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }
}