package com.stratio.cassandra.lucene.bitemporal;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.querytype.AbstractWatchedTest;
import com.stratio.cassandra.lucene.schema.SchemaBuilder;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.CassandraUtilsSelect;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.bitemporalMapper;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.schema;
import static com.stratio.cassandra.lucene.search.SearchBuilders.bitemporalSearch;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class BitemporalTest {

    private static final Logger logger = Logger.getLogger(AbstractWatchedTest.class);

    private static long startingTime;
    private static final String TIMESTAMP_PATTERN="timestamp";
    private static final String SIMPLE_DATE_PATTERN="yyyy/MM/dd HH:mm:ss.SSS";

    protected static CassandraUtils cassandraUtils;

    public static final Map<String, String> data1;
    public static final Map<String, String> data2;
    public static final Map<String, String> data3;
    public static final Map<String, String> data4;
    public static final Map<String, String> data5;
    public static final Map<String, String> data6;
    public static final Map<String, String> data7;
    public static final Map<String, String> data8;
    public static final Map<String, String> data9;
    public static final Map<String, String> data10;
    public static final Map<String, String> data11;

    static {

        data1 = new LinkedHashMap<>();
        data1.put("integer_1", "1");
        //yyyy/MM/dd HH:mm:ss.SSS
        data1.put("vt_from", "'2015/01/01 00:00:00.000'");
        data1.put("vt_to", "'2015/02/01 12:00:00.000'");
        data1.put("tt_from", "'2015/01/15 12:00:00.001'");
        data1.put("tt_to", "'2015/02/15 12:00:00.000'");

        data2 = new LinkedHashMap<>();
        data2.put("integer_1", "2");
        data2.put("vt_from", "'2015/02/01 12:00:00.001'");
        data2.put("vt_to", "'2015/03/01 12:00:00.000'");
        data2.put("tt_from", "'2015/02/15 12:00:00.001'");
        data2.put("tt_to", "'2015/03/15 12:00:00.000'");

        data3 = new LinkedHashMap<>();
        data3.put("integer_1", "3");
        data3.put("vt_from", "'2015/03/01 12:00:00.001'");
        data3.put("vt_to", "'2015/04/01 12:00:00.000'");
        data3.put("tt_from", "'2015/03/15 12:00:00.001'");
        data3.put("tt_to", "'2015/04/15 12:00:00.000'");

        data4 = new LinkedHashMap<>();
        data4.put("integer_1", "4");
        data4.put("vt_from", "'2015/04/01 12:00:00.001'");
        data4.put("vt_to", "'2015/05/01 12:00:00.000'");
        data4.put("tt_from", "'2015/04/15 12:00:00.001'");
        data4.put("tt_to", "'2015/05/15 12:00:00.000'");

        data5 = new LinkedHashMap<>();
        data5.put("integer_1", "5");
        data5.put("vt_from", "'2015/05/01 12:00:00.001'");
        data5.put("vt_to", "'2015/06/01 12:00:00.000'");
        data5.put("tt_from", "'2015/05/15 12:00:00.001'");
        data5.put("tt_to", "'2015/06/15 12:00:00.000'");

        data6 = new LinkedHashMap<>();
        data6.put("integer_1", "5");
        data6.put("vt_from", "'2016/05/01 12:00:00.001'");
        data6.put("vt_to", "'2016/06/01 12:00:00.000'");
        data6.put("tt_from", "'2016/05/15 12:00:00.001'");
        data6.put("tt_to", "'2016/06/15 12:00:00.000'");

        data7 = new LinkedHashMap<>();
        data7.put("id", "1");
        data7.put("data", "'v1'");
        data7.put("vt_from", "0");
        data7.put("vt_to", "9223372036854775807");
        data7.put("tt_from", "0");
        data7.put("tt_to", "9223372036854775807");

        data8 = new LinkedHashMap<>();
        data8.put("id", "2");
        data8.put("data", "'v1'");
        data8.put("vt_from", "0");
        data8.put("vt_to", "9223372036854775807");
        data8.put("tt_from", "0");
        data8.put("tt_to", "9223372036854775807");

        data9 = new LinkedHashMap<>();
        data9.put("id", "3");
        data9.put("data", "'v1'");
        data9.put("vt_from", "0");
        data9.put("vt_to", "9223372036854775807");
        data9.put("tt_from", "0");
        data9.put("tt_to", "9223372036854775807");

        data10 = new LinkedHashMap<>();
        data10.put("id", "4");
        data10.put("data", "'v1'");
        data10.put("vt_from", "0");
        data10.put("vt_to", "9223372036854775807");
        data10.put("tt_from", "0");
        data10.put("tt_to", "9223372036854775807");

        data11 = new LinkedHashMap<>();
        data11.put("id", "5");
        data11.put("data", "'v1'");
        data11.put("vt_from", "0");
        data11.put("vt_to", "9223372036854775807");
        data11.put("tt_from", "0");
        data11.put("tt_to", "9223372036854775807");

    }

    @Rule
    public TestRule watcher = new TestWatcher() {

        protected void starting(Description description) {
            logger.info("***************** Running test [" + description.getMethodName() + "]");
            startingTime = System.currentTimeMillis();
        }

        protected void finished(Description description) {
            logger.info("***************** Tested in [" + (System.currentTimeMillis() - startingTime) + " ms]");
        }
    };

    @BeforeClass
    public static void setUpSuite() throws InterruptedException {

        SchemaBuilder schemaBuilder = schema().mapper("bitemporal",
                                                      bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").pattern(
                                                              "yyyy/MM/dd HH:mm:ss.SSS"));

        cassandraUtils = CassandraUtils.builder()
                                       .withHost(TestingConstants.CASSANDRA_LOCALHOST_CONSTANT)
                                       .withTable(TestingConstants.TABLE_NAME_CONSTANT)
                                       .withIndexColumn(TestingConstants.INDEX_COLUMN_CONSTANT)
                                       .withPartitionKey("integer_1")
                                       .withClusteringKey()
                                       .withColumn("integer_1", "int")
                                       .withColumn("vt_from", "text")
                                       .withColumn("vt_to", "text")
                                       .withColumn("tt_from", "text")
                                       .withColumn("tt_to", "text")
                                       .withColumn("lucene", "text")
                                       .build();

        cassandraUtils.createKeyspace()
                      .createTable()
                      .createIndex(TestingConstants.INDEX_NAME_CONSTANT, schemaBuilder)
                      .insert(data1, data2, data3, data4, data5);
        System.out.println("finished seting up the testSuite");
    }

    @AfterClass
    public static void tearDownSuite() {
        cassandraUtils.dropIndex(TestingConstants.INDEX_NAME_CONSTANT)

                      .dropTable()

                      .dropKeyspace();

        System.out.println("tearDownSuite");
    }

    private String fromInteger(Integer[] list) {

        String out = "{";
        for (Integer aList : list) {
            out += Integer.toString(aList) + ",";
        }
        return out.substring(0, out.length() - 1) + "}";

    }

    private boolean isThisAndOnlyThis(Integer[] intList1, int[] intList2) {
        if (intList1.length != intList2.length) {
            return false;
        } else {

            for (Integer i : intList1) {
                boolean found = false;
                for (Integer j : intList2) {
                    if (i.equals(j)) {
                        found = true;
                    }
                }
                if (!found) return false;
            }
            return true;
        }
    }

    @Test
    public void bitemporalQueryIsWithInTimeStampFieldTest() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("is_within")
                                                                                         .vtFrom("2014/12/31 12:00:00.000")
                                                                                         .vtTo("2015/02/02 00:00:00.000")
                                                                                         .ttFrom("2015/01/14 00:00:00.000")
                                                                                         .ttTo("2015/02/16 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {1}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1}));
    }

    @Test
    public void bitemporalQueryIsWithInTimeStampFieldTest2() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("is_within")
                                                                                         .vtFrom("2015/01/31 12:00:00.000")
                                                                                         .vtTo("2015/03/02 00:00:00.000")
                                                                                         .ttFrom("2015/02/14 00:00:00.000")
                                                                                         .ttTo("2015/03/16 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{2}));
    }

    @Test
    public void bitemporalQueryIsWithInTimeStampFieldTest3() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("is_within")
                                                                                         .vtFrom("2015/02/28 12:00:00.000")
                                                                                         .vtTo("2015/04/02 00:00:00.000")
                                                                                         .ttFrom("2015/03/14 00:00:00.000")
                                                                                         .ttTo("2015/04/16 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{3}));
    }

    @Test
    public void bitemporalQueryIsWithInTimeStampFieldTest4() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("is_within")
                                                                                         .vtFrom("2015/03/31 12:00:00.000")
                                                                                         .vtTo("2015/05/02 00:00:00.000")
                                                                                         .ttFrom("2015/04/14 00:00:00.000")
                                                                                         .ttTo("2015/05/16 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {4}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{4}));
    }

    @Test
    public void bitemporalQueryIsWithInTimeStampFieldTes5() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("is_within")
                                                                                         .vtFrom("2015/04/31 12:00:00.000")
                                                                                         .vtTo("2015/06/02 00:00:00.000")
                                                                                         .ttFrom("2015/05/14 00:00:00.000")
                                                                                         .ttTo("2015/06/16 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {5}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{5}));
    }

    @Test
    public void bitemporalQueryIsWithInTimeStampFieldTes6() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("is_within")
                                                                                         .vtFrom("2014/12/31 12:00:00.000")
                                                                                         .vtTo("2015/03/02 00:00:00.000")
                                                                                         .ttFrom("2015/01/14 00:00:00.000")
                                                                                         .ttTo("2015/04/02 00:00:00.000"));

        assertEquals("Expected 2 results!", 2, select.count());
        assertTrue("Unexpected results!! Expected: {1,2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2}));
    }

    @Test
    public void bitemporalQueryIsWithInTimeStampFieldTes7() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("is_within")
                                                                                         .vtFrom("2015/03/02 12:00:00.000")
                                                                                         .vtTo("2015/03/10 00:00:00.000")
                                                                                         .ttFrom("2015/01/14 00:00:00.000")
                                                                                         .ttTo("2015/04/02 00:00:00.000"));

        assertEquals("Expected 0 results!", 0, select.count());
        assertTrue("Unexpected results!! Expected: {}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{}));
    }

    @Test
    public void bitemporalQueryIsWithInTimeStampFieldTes8() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("is_within")
                                                                                         .vtFrom("2014/03/02 12:00:00.000")
                                                                                         .vtTo("2015/03/10 00:00:00.000")
                                                                                         .ttFrom("2015/01/14 00:00:00.000")
                                                                                         .ttTo("2015/01/16 00:00:00.000"));

        assertEquals("Expected 0 results!", 0, select.count());
        assertTrue("Unexpected results!! Expected: {}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{}));
    }

    @Test
    public void bitemporalQueryIsWithInTimeStampFieldTest9() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("is_within")
                                                                                         .vtFrom("2014/12/31 12:00:00.000")
                                                                                         .vtTo("2015/06/02 00:00:00.000")
                                                                                         .ttFrom("2015/01/14 00:00:00.000")
                                                                                         .ttTo("2015/06/16 00:00:00.000"));

        assertEquals("Expected 5 results!", 5, select.count());
        assertTrue("Unexpected results!! Expected: {1,2,3,4,5}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2, 3, 4, 5}));
    }

    @Test
    public void bitemporalQueryContainsTimeStampFieldTest() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("contains")
                                                                                         .vtFrom("2015/01/02 12:00:00.000")
                                                                                         .vtTo("2015/01/30 00:00:00.000")
                                                                                         .ttFrom("2015/01/16 00:00:00.000")
                                                                                         .ttTo("2015/02/14 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {1}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1}));
    }

    @Test
    public void bitemporalQueryContainsTimeStampFieldTest2() {
        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("contains")
                                                                                         .vtFrom("2015/02/02 12:00:00.000")
                                                                                         .vtTo("2015/02/28 00:00:00.000")
                                                                                         .ttFrom("2015/02/16 00:00:00.000")
                                                                                         .ttTo("2015/03/14 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{2}));
    }

    @Test
    public void bitemporalQueryContainsTimeStampFieldTest3() {
        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("contains")
                                                                                         .vtFrom("2015/03/02 12:00:00.000")
                                                                                         .vtTo("2015/03/30 00:00:00.000")
                                                                                         .ttFrom("2015/03/16 00:00:00.000")
                                                                                         .ttTo("2015/04/14 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{3}));
    }

    @Test
    public void bitemporalQueryContainsTimeStampFieldTest4() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("contains")
                                                                                         .vtFrom("2015/04/02 12:00:00.000")
                                                                                         .vtTo("2015/04/30 00:00:00.000")
                                                                                         .ttFrom("2015/04/16 00:00:00.000")
                                                                                         .ttTo("2015/05/14 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {4}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{4}));
    }

    @Test
    public void bitemporalQueryContainsTimeStampFieldTest5() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("contains")
                                                                                         .vtFrom("2015/05/02 12:00:00.000")
                                                                                         .vtTo("2015/05/30 00:00:00.000")
                                                                                         .ttFrom("2015/05/16 00:00:00.000")
                                                                                         .ttTo("2015/06/14 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {5}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{5}));
    }

    @Test
    public void bitemporalQueryContainsTimeStampFieldTest6() {
        //vt out of any example, tt inside first data, it must return 0

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("contains")
                                                                                         .vtFrom("2014/10/31 12:00:00.000")
                                                                                         .vtTo("2014/11/02 00:00:00.000")
                                                                                         .ttFrom("2015/01/14 00:00:00.000")
                                                                                         .ttTo("2015/06/16 00:00:00.000"));

        assertEquals("Expected 0 results!", 0, select.count());
        assertTrue("Unexpected results!! Expected: {}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{}));
    }

    @Test
    public void bitemporalQueryContainsTimeStampFieldTest7() {
        //vt inside a  data example, tt outside any data, it must return 0

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("contains")
                                                                                         .vtFrom("2015/05/02 12:00:00.000")
                                                                                         .vtTo("2015/05/30 00:00:00.000")
                                                                                         .ttFrom("2014/05/16 00:00:00.000")
                                                                                         .ttTo("2014/06/14 00:00:00.000"));

        assertEquals("Expected 0 results!", 0, select.count());
        assertTrue("Unexpected results!! Expected: {}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{}));
    }

    @Test
    public void bitemporalQueryIntersectsTimeStampFieldTest() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("intersects")
                                                                                         .vtFrom("2015/01/01 00:00:00.000")
                                                                                         .vtTo("2015/02/01 12:00:00.000")
                                                                                         .ttFrom("2015/01/15 12:00:00.001")
                                                                                         .ttTo("2015/02/15 12:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {1}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1}));
    }

    @Test
    public void bitemporalQueryIntersectsTimeStampFieldTest2() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("intersects")
                                                                                         .vtFrom("2015/02/01 12:00:00.001")
                                                                                         .vtTo("2015/03/01 12:00:00.000")
                                                                                         .ttFrom("2015/02/15 12:00:00.001")
                                                                                         .ttTo("2015/03/15 12:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{2}));
    }

    @Test
    public void bitemporalQueryIntersectsTimeStampFieldTest3() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("intersects")
                                                                                         .vtFrom("2015/03/01 12:00:00.001")
                                                                                         .vtTo("2015/04/01 12:00:00.000")
                                                                                         .ttFrom("2015/03/15 12:00:00.001")
                                                                                         .ttTo("2015/04/15 12:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{3}));
    }

    @Test
    public void bitemporalQueryIntersectsTimeStampFieldTest4() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("intersects")
                                                                                         .vtFrom("2015/04/01 12:00:00.001")
                                                                                         .vtTo("2015/05/01 12:00:00.000")
                                                                                         .ttFrom("2015/04/15 12:00:00.001")
                                                                                         .ttTo("2015/05/15 12:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {4}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{4}));
    }

    @Test
    public void bitemporalQueryIntersectsTimeStampFieldTest5() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("intersects")
                                                                                         .vtFrom("2015/05/01 12:00:00.001")
                                                                                         .vtTo("2015/06/01 12:00:00.000")
                                                                                         .ttFrom("2015/05/15 12:00:00.001")
                                                                                         .ttTo("2015/06/15 12:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {5}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{5}));
    }

    @Test
    public void bitemporalQueryIntersectsTimeStampFieldTest6() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("intersects")
                                                                                         .vtFrom("2014/12/31 12:00:00.000")
                                                                                         .vtTo("2015/03/02 00:00:00.000")
                                                                                         .ttFrom("2015/01/14 00:00:00.000")
                                                                                         .ttTo("2015/04/02 00:00:00.000"));

        assertEquals("Expected 3 results!", 3, select.count());
        assertTrue("Unexpected results!! Expected: {1,2,3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2, 3}));
    }

    @Test
    public void bitemporalQueryIntersectsTimeStampFieldTest7() {
        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("intersects")
                                                                                         .vtFrom("2014/12/01 12:00:00.000")
                                                                                         .vtTo("2014/12/31 00:00:00.000")
                                                                                         .ttFrom("2015/01/14 00:00:00.000")
                                                                                         .ttTo("2015/04/02 00:00:00.000"));

        assertEquals("Expected 0 results!", 0, select.count());
        assertTrue("Unexpected results!! Expected: {}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{}));
    }

    @Test
    public void bitemporalQueryIntersectsTimeStampFieldTest8() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("intersects")
                                                                                         .vtFrom("2015/01/01 00:00:00.000")
                                                                                         .vtTo("2015/02/01 12:00:00.001")
                                                                                         .ttFrom("2015/01/15 12:00:00.001")
                                                                                         .ttTo("2015/02/15 12:00:00.001"));

        assertEquals("Expected 2 results!", 2, select.count());
        assertTrue("Unexpected results!! Expected: {1,2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2}));
    }

    @Test
    public void bitemporalQueryIntersectsTimeStampFieldTest9() {

        CassandraUtilsSelect select = cassandraUtils.query(bitemporalSearch("bitemporal").operation("intersects")
                                                                                         .vtFrom("2015/02/01 12:00:00.000")
                                                                                         .vtTo("2015/03/01 12:00:00.000")
                                                                                         .ttFrom("2015/02/15 12:00:00.000")
                                                                                         .ttTo("2015/03/15 12:00:00.000"));

        assertEquals("Expected 2 results!", 2, select.count());
        assertTrue("Unexpected results!! Expected: {1,2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2}));
    }

    private CassandraUtils setUpSuite2(Object nowValue,String pattern) {

        SchemaBuilder schemaBuilder = schema().mapper("bitemporal",bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to").pattern(
                                                              pattern).nowValue(nowValue));

        CassandraUtils cu = CassandraUtils.builder()
                                          .withHost(TestingConstants.CASSANDRA_LOCALHOST_CONSTANT)
                                          .withTable(TestingConstants.TABLE_NAME_CONSTANT)
                                          .withIndexColumn(TestingConstants.INDEX_COLUMN_CONSTANT)
                                          .withPartitionKey("integer_1")
                                          .withClusteringKey()
                                          .withColumn("integer_1", "int")
                                          .withColumn("vt_from", "text")
                                          .withColumn("vt_to", "text")
                                          .withColumn("tt_from", "text")
                                          .withColumn("tt_to", "text")
                                          .withColumn("lucene", "text")
                                          .build();

        cu.createKeyspace().createTable().createIndex(TestingConstants.INDEX_NAME_CONSTANT, schemaBuilder);

        return cu;
    }

    private CassandraUtils setUpSuite3() {

        SchemaBuilder schemaBuilder = schema().mapper("bitemporal",
                                                      bitemporalMapper("vt_from", "vt_to", "tt_from", "tt_to")
                                                              .pattern(TIMESTAMP_PATTERN));

        CassandraUtils cu = CassandraUtils.builder()
                                          .withHost(TestingConstants.CASSANDRA_LOCALHOST_CONSTANT)
                                          .withTable(TestingConstants.TABLE_NAME_CONSTANT)
                                          .withIndexColumn(TestingConstants.INDEX_COLUMN_CONSTANT)
                                          .withPartitionKey("id")
                                          .withClusteringKey("vt_from", "tt_from")
                                          .withColumn("id", "int")
                                          .withColumn("data", "text")
                                          .withColumn("vt_from", "bigint")
                                          .withColumn("vt_to", "bigint")
                                          .withColumn("tt_from", "bigint")
                                          .withColumn("tt_to", "bigint")
                                          .withColumn("lucene", "text")
                                          .build();

        cu.createKeyspace().createTable().createIndex(TestingConstants.INDEX_NAME_CONSTANT, schemaBuilder);

        return cu;

    }

    private void tearDown(CassandraUtils cu) {
        cu.dropIndex(TestingConstants.INDEX_NAME_CONSTANT)

          .dropTable()

          .dropKeyspace();

    }

    //valid long max value queries
    @Test
    public void bitemporalQueryIsWithInNowValueToLongTest() {
        //testing with long value 1456876800 ==2016/03/02 00:00:00

        String nowValue = "2016/03/02 00:00:00.000";
        CassandraUtils cu = this.setUpSuite2(nowValue,SIMPLE_DATE_PATTERN);
        cu.insert(data1, data2, data3, data4, data5);
        CassandraUtilsSelect select = cu.query(bitemporalSearch("bitemporal").operation("is_within")
                                                                             .vtFrom("2015/02/28 12:00:00.000")
                                                                             .vtTo("2015/04/02 00:00:00.000")
                                                                             .ttFrom("2015/03/14 00:00:00.000")
                                                                             .ttTo("2015/04/16 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{3}));
        tearDown(cu);
    }

    //inserting bigger to nowValue it
    @Test
    public void bitemporalQueryIsWithInNowValueToLongTest2() {
        //testing with long value 1456876800 ==2016/03/02 00:00:00
        String nowValue = "2016/03/02 00:00:00.000";
        CassandraUtils cu = this.setUpSuite2(nowValue,SIMPLE_DATE_PATTERN);

        cu.insert(data6);

        tearDown(cu);
    }

    //querying bigger than nowValue it must throw an IllegalArgumentException
    @Test(expected = InvalidQueryException.class)
    public void bitemporalQueryIsWithInNowValueToLongTest3() {
        //testing with long value 1456876800 ==2016/03/02 00:00:00

        String nowValue = "2016/03/02 00:00:00.000";
        CassandraUtils cu = this.setUpSuite2(nowValue,SIMPLE_DATE_PATTERN);
        cu.insert(data1, data2, data3, data4, data5);

        cu.query(bitemporalSearch("bitemporal").operation("is_within")
                                               .vtFrom("2016/02/28 12:00:00.000")
                                               .vtTo("2016/04/02 00:00:00.000")
                                               .ttFrom("2016/03/14 00:00:00.000")
                                               .ttTo("2016/04/16 00:00:00.000")).count();
        tearDown(cu);
    }

    //valid String max value queries
    @Test
    public void bitemporalQueryIsWithInNowValueToStringTest() {
        //testing with string value
        String nowValue = "2016/03/02 00:00:00.000";
        CassandraUtils cu = this.setUpSuite2(nowValue, SIMPLE_DATE_PATTERN)
                                .insert(data1, data2, data3, data4, data5);

        //testing if inserting data translate it to Long.max
        CassandraUtilsSelect select = cu.query(bitemporalSearch("bitemporal").operation("is_within")
                                                                             .vtFrom("2015/02/28 12:00:00.000")
                                                                             .vtTo("2015/04/02 00:00:00.000")
                                                                             .ttFrom("2015/03/14 00:00:00.000")
                                                                             .ttTo("2015/04/16 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{3}));
        tearDown(cu);
    }

    //inserting bigger to nowValue it must throw an IllegalArgumentException
    @Test
    public void bitemporalQueryIsWithInNowValueToStringTest2() {
        //testing with long value
        String nowValue = "2016/03/02 00:00:00.000";
        CassandraUtils cu = this.setUpSuite2(nowValue,SIMPLE_DATE_PATTERN);

        //testing if inserting data translate it to Long.max
        cu.insert(data6);

        tearDown(cu);
    }

    //querying bigger than nowValue it must throw an IllegalArgumentException
    @Test(expected = InvalidQueryException.class)
    public void bitemporalQueryIsWithInNowValueToStringTest3() {
        //testing with long value
        String nowValue = "2016/03/02 00:00:00.000";

        CassandraUtils cu = this.setUpSuite2(nowValue,SIMPLE_DATE_PATTERN)
                                .insert(data1, data2, data3, data4, data5);

        cu.query(bitemporalSearch("bitemporal").operation("is_within")
                                               .vtFrom("2016/02/28 12:00:00.000")
                                               .vtTo("2016/04/02 00:00:00.000")
                                               .ttFrom("2016/03/14 00:00:00.000")
                                               .ttTo("2016/04/16 00:00:00.000")).count();
        //testing if inserting data translate it to Long.max

        tearDown(cu);
    }

    //valid String max value queries settign nowValue to max date in data3
    @Test
    public void bitemporalQueryIsWithInNowValueToStringTest4() {
        //testing with string value
        String nowValue = "2015/04/15 12:00:00.000";

        CassandraUtils cu = this.setUpSuite2(nowValue, SIMPLE_DATE_PATTERN)
                                .insert(data1, data2, data3);

        //testing if inserting data translate it to Long.max

        CassandraUtilsSelect select = cu.query(bitemporalSearch("bitemporal").operation("intersects")
                                                                             .vtFrom("2014/12/31 12:00:00.000")
                                                                             .vtTo("2015/03/02 00:00:00.000")
                                                                             .ttFrom("2015/01/14 00:00:00.000")
                                                                             .ttTo("2015/04/02 00:00:00.000"));

        assertEquals("Expected 3 results!", 3, select.count());
        assertTrue("Unexpected results!! Expected: {1,2,3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2, 3}));
    }

    //querying without limits to vt
    @Test
    public void bitemporalQueryIsWithInNowValueToStringTest5() {
        //testing with string value
        String nowValue = "2015/04/15 12:00:00.000";

        CassandraUtils cu = this.setUpSuite2(nowValue, SIMPLE_DATE_PATTERN)
                                .insert(data1, data2, data3);

        //testing if inserting data translate it to Long.max

        CassandraUtilsSelect select = cu.query(bitemporalSearch("bitemporal").operation("intersects")
                                                                             .ttFrom("2015/01/14 00:00:00.000")
                                                                             .ttTo("2015/04/02 00:00:00.000"));

        assertEquals("Expected 3 results!", 3, select.count());
        assertTrue("Unexpected results!! Expected: {1,2,3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2, 3}));
    }

    //querying without limits to tt
    @Test
    public void bitemporalQueryIsWithInNowValueToStringTest6() {
        //testing with string value
        String nowValue = "2015/04/15 12:00:00.000";

        CassandraUtils cu = this.setUpSuite2(nowValue, SIMPLE_DATE_PATTERN)
                                .insert(data1, data2, data3);

        //testing if inserting data translate it to Long.max

        CassandraUtilsSelect select = cu.query(bitemporalSearch("bitemporal").operation("intersects")
                                                                             .vtFrom("2014/12/31 12:00:00.000")
                                                                             .vtTo("2015/03/02 00:00:00.000"));

        assertEquals("Expected 3 results!", 3, select.count());
        assertTrue("Unexpected results!! Expected: {1,2,3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2, 3}));
    }

    @Test
    public void bitemporalQueryOverBigIntsWithDefaultPattern() {
        CassandraUtils cu = this.setUpSuite3().insert(data7, data8, data9, data10, data11);

        CassandraUtilsSelect select = cu.searchAll();

        assertEquals("Expected 5 results!", 5, select.count());
        assertTrue("Unexpected results!! Expected: {1,2,3,4,5}, got: " + fromInteger(select.intColumn("id")),
                   isThisAndOnlyThis(select.intColumn("id"), new int[]{1, 2, 3, 4, 5}));

        Batch batch = QueryBuilder.batch();
        Update update = QueryBuilder.update(cu.getKeyspace(), cu.getTable());

        update.where(QueryBuilder.eq("id", 1))
              .and(QueryBuilder.eq("vt_from", 0))
              .and(QueryBuilder.eq("tt_from", 0))
              .onlyIf(QueryBuilder.eq("tt_to", 9223372036854775807l))
              .with(QueryBuilder.set("tt_to", 20150101));

        batch.add(update);

        Insert insert = QueryBuilder.insertInto(cu.getKeyspace(), cu.getTable());
        insert.value("id", 1)
              .value("data", "v2")
              .value("vt_from", 0)
              .value("vt_to", 9223372036854775807l)
              .value("tt_from", 20150102)
              .value("tt_to", 9223372036854775807l);

        batch.add(insert);
        ResultSet result = cu.getSession().execute(batch);

        assertTrue("batch execution didnt worked", result.wasApplied());

        CassandraUtilsSelect select2 = cu.filter(bitemporalSearch("bitemporal").operation("intersects")
                                                                               .vtFrom(0)
                                                                               .vtTo(9223372036854775807l)
                                                                               .ttFrom(9223372036854775807l)
                                                                               .ttTo(9223372036854775807l))
                                                .refresh(true);


        List<Row> results=select2.get();
        System.out.println("RESULTS: ");

        for (Row row: results)  {
            StringBuilder stringBuilder= new StringBuilder();
            stringBuilder.append("ROW[");
            stringBuilder.append("id="+Integer.toString(row.getInt("id"))+", ");
            stringBuilder.append("data="+row.getString("data")+", ");

            stringBuilder.append("vt_from="+Long.toString(row.getLong("vt_from"))+", ");
            stringBuilder.append("vt_to="+Long.toString(row.getLong("vt_to"))+", ");
            stringBuilder.append("tt_from="+Long.toString(row.getLong("tt_from"))+", ");
            stringBuilder.append("tt_to="+Long.toString(row.getLong("tt_to"))+" ];");
            System.out.println(stringBuilder.toString());
        }

        assertEquals("Expected 5 results!", 5, select2.count());
        assertTrue("Unexpected results!! Expected: {1,2,3,4,5}, got: " + fromInteger(select2.intColumn("id")),
                   isThisAndOnlyThis(select2.intColumn("id"), new int[]{1, 2, 3, 4, 5}));

        CassandraUtilsSelect select3 = cu.filter(bitemporalSearch("bitemporal").operation("intersects")
                                                                               .vtFrom(0)
                                                                               .vtTo(9223372036854775807l)
                                                                               .ttFrom(9223372036854775807l)
                                                                               .ttTo(9223372036854775807l))
                                         .and("AND id = 1");

        assertEquals("Expected 1 results!", 1, select3.count());
        Row row = select3.get().get(0);

        assertTrue("Unexpected results!! Expected result : {id=\"1\"}, got: " + row.getInt("id"),
                   row.getInt("id") == 1);
        assertTrue("Unexpected results!! Expected result : {data=\"v2\"}, got: " + row.getString("data"),
                   row.getString("data").equals("v2"));
        assertTrue("Unexpected results!! Expected result : {vt_from=0}, got: " + row.getLong("vt_from"),
                   row.getLong("vt_from") == 0l);
        assertTrue("Unexpected results!! Expected result : {vt_to=0}, got: " + row.getLong("vt_to"),
                   row.getLong("vt_to") == 9223372036854775807l);
        assertTrue("Unexpected results!! Expected result : {tt_from=0}, got: " + row.getLong("tt_from"),
                   row.getLong("tt_from") == 20150102l);
        assertTrue("Unexpected results!! Expected result : {tt_to=0}, got: " + row.getLong("tt_to"),
                   row.getLong("tt_to") == 9223372036854775807l);

    }
}
