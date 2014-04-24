package com.stratio.cassandra.lucene.querytype;

/**
 * Created by Jcalderin on 24/03/14.
 */

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.stratio.cassandra.lucene.TestingConstants;

@RunWith(JUnit4.class)
public class CollectionsTest extends AbstractWatchedTest {

    @Test(expected = InvalidQueryException.class)
    public void matchListFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("list_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchListFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("list_1", "l1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void matchListFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("list_1", "s1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test(expected = InvalidQueryException.class)
    public void matchSetFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("set_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchSetFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("set_1", "l1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchSetFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("set_1", "s1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test(expected = InvalidQueryException.class)
    public void matchMapFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("map_1.k1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchMapFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("map_1.k1", "l1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchMapFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("map_1.k1", "k1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void matchMapFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getMatchQuery("map_1.k1", "v1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test(expected = InvalidQueryException.class)
    public void fuzzyListFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("list_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void fuzzyListFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("list_1", "l1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void fuzzyListFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("list_1", "s1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void fuzzyListFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("list_1", "s7l", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test(expected = InvalidQueryException.class)
    public void fuzzySetFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("set_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void fuzzySetFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("set_1", "l1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void fuzzySetFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("set_1", "s1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void fuzzySetFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("set_1", "k87", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test(expected = InvalidQueryException.class)
    public void fuzzyMapFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("map_1.k1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void fuzzyMapFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("map_1.k1", "l1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void fuzzyMapFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("map_1.k1", "k1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void fuzzyMapFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getFuzzyQuery("map_1.k1", "v1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void phraseListFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPhraseQuery("list_1", null, null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void phraseListFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPhraseQuery("list_1", Arrays.asList("l1"), null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void phraseListFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPhraseQuery("list_1", Arrays.asList("l1", "l2"), null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void phraseListFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPhraseQuery("list_1", Arrays.asList("s1"), null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void phraseSetFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPhraseQuery("set_1", null, null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void phraseSetFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPhraseQuery("set_1", Arrays.asList("l1"), null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void phraseSetFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPhraseQuery("set_1", Arrays.asList("s1"), null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void phraseMapFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPhraseQuery("map_1.k1", null, null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void phraseMapFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPhraseQuery("map_1.k1", Arrays.asList("l1"), null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void phraseMapFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPhraseQuery("map_1.k1", Arrays.asList("k1"), null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void phraseMapFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPhraseQuery("map_1.k1", Arrays.asList("v1"), null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void prefixListFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("list_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void prefixListFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("list_1", "l1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void prefixListFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("list_1", "l", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void prefixListFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("list_1", "s1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void prefixSetFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("set_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void prefixSetFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("set_1", "l1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void prefixSetFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("set_1", "s1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void prefixMapFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("map_1.k1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void prefixMapFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("map_1.k1", "l1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void prefixMapFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("map_1.k1", "k1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void prefixMapFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getPrefixQuery("map_1.k1", "v1", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void rangeListFieldTest1() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "z");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("list_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeListFieldTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a1");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "z9");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("list_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeListFieldTest3() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a1");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "l1");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("list_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void rangeSetFieldTest1() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "z");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("set_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeSetFieldTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a1");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "z9");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("set_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void rangeSetFieldTest3() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a1");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "z1");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("set_1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void rangeMapFieldTest1() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "z");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("map_1.k1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void rangeMapFieldTest2() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a1");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "z9");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("map_1.k1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void rangeMapFieldTest3() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a1");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "k9");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("map_1.k1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void rangeMapFieldTest4() {

        Map<String, String> params = new LinkedHashMap<>();
        params.put(TestingConstants.LOWER_PARAM_CONSTANT, "a1");
        params.put(TestingConstants.UPPER_PARAM_CONSTANT, "k1");

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRangeQuery("map_1.k1", params));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test(expected = InvalidQueryException.class)
    public void regexpListFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("list_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void regexpListFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("list_1", "l.*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void regexpListFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("list_1", "s.*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test(expected = InvalidQueryException.class)
    public void regexpSetFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("set_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void regexpSetFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("set_1", "l.*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void regexpSetFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("set_1", "s.*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test(expected = InvalidQueryException.class)
    public void regexpMapFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("map_1.k1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void regexpMapFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("map_1.k1", "l.*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void regexpMapFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("map_1.k1", "k.*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test
    public void regexpMapFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getRegexpQuery("map_1.k1", "v.*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }

    @Test(expected = InvalidQueryException.class)
    public void wildcardListFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("list_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void wildcardListFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("list_1", "l*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test
    public void wildcardListFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("list_1", "s*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test(expected = InvalidQueryException.class)
    public void wildcardSetFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("set_1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void wildcardSetFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("set_1", "l*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void wildcardSetFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("set_1", "s*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 4 results!", 4, rows.size());
    }

    @Test(expected = InvalidQueryException.class)
    public void wildcardMapFieldTest1() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("map_1.k1", "", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void wildcardMapFieldTest2() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("map_1.k1", "l*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void wildcardMapFieldTest3() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("map_1.k1", "k*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 0 results!", 0, rows.size());
    }

    @Test
    public void wildcardMapFieldTest4() {

        ResultSet queryResult = cassandraUtils.executeQuery(queryUtils
                .getWildcardQuery("map_1.k1", "v*", null));

        List<Row> rows = queryResult.all();

        assertEquals("Expected 2 results!", 2, rows.size());
    }
}