package com.stratio.cassandra.lucene.querytype;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.querybuilder.*;
import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.CassandraUtilsSelect;
import org.apache.cassandra.cql.UpdateStatement;
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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.search.SearchBuilders.biTemporalSearch;
import static com.stratio.cassandra.lucene.search.SearchBuilders.matchAll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by eduardoalonso on 30/06/15.
 */
class DataHelper {

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

    /*
    public static final Map<String, String> data12;
    public static final Map<String, String> data13;
    public static final Map<String, String> data14;
    public static final Map<String, String> data15;
    public static final Map<String, String> data16;
    public static final Map<String, String> data17;

    public static final Map<String, String> data18;
    public static final Map<String, String> data19;
*/


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

        data6= new LinkedHashMap<>();
        data6.put("integer_1", "5");
        data6.put("vt_from", "'2016/05/01 12:00:00.001'");
        data6.put("vt_to", "'2016/06/01 12:00:00.000'");
        data6.put("tt_from", "'2016/05/15 12:00:00.001'");
        data6.put("tt_to", "'2016/06/15 12:00:00.000'");



        data7= new LinkedHashMap<>();
        data7.put("id", "1");
        data7.put("data", "'v1'");
        data7.put("vt_from", "0");
        data7.put("vt_to", "9223372036854775807");
        data7.put("tt_from", "0");
        data7.put("tt_to", "9223372036854775807");


        data8= new LinkedHashMap<>();
        data8.put("id", "2");
        data8.put("data", "'v1'");
        data8.put("vt_from", "0");
        data8.put("vt_to", "9223372036854775807");
        data8.put("tt_from", "0");
        data8.put("tt_to", "9223372036854775807");


        data9= new LinkedHashMap<>();
        data9.put("id", "3");
        data9.put("data", "'v1'");
        data9.put("vt_from", "0");
        data9.put("vt_to", "9223372036854775807");
        data9.put("tt_from", "0");
        data9.put("tt_to", "9223372036854775807");


        data10= new LinkedHashMap<>();
        data10.put("id", "4");
        data10.put("data", "'v1'");
        data10.put("vt_from", "0");
        data10.put("vt_to", "9223372036854775807");
        data10.put("tt_from", "0");
        data10.put("tt_to", "9223372036854775807");

        data11= new LinkedHashMap<>();
        data11.put("id", "5");
        data11.put("data", "'v1'");
        data11.put("vt_from", "0");
        data11.put("vt_to", "9223372036854775807");
        data11.put("tt_from", "0");
        data11.put("tt_to", "9223372036854775807");




    }
}

@RunWith(JUnit4.class)
public class BiTemporalTest {

    private static final Logger logger = Logger.getLogger(AbstractWatchedTest.class);

    private static long startingTime;

    protected static CassandraUtils cassandraUtils;



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
        Map<String, String> fieldsMap = new HashMap<>();

        fieldsMap.put("bitemporal",
                      "{type:\"bitemporal\",tt_from:\"tt_from\", tt_to:\"tt_to\",vt_from:\"vt_from\", vt_to:\"vt_to\",pattern:\"yyyy/MM/dd HH:mm:ss.SSS\"}");

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
                      .createCustomIndex(TestingConstants.INDEX_NAME_CONSTANT, fieldsMap)
                      .insert(DataHelper.data1)
                      .insert(DataHelper.data2)
                      .insert(DataHelper.data3)
                      .insert(DataHelper.data4)
                      .insert(DataHelper.data5)
                      .waitForIndexRefresh();
        System.out.println("finished seting up the testSuite");
    }

    @AfterClass
    public static void tearDownSuite() {
        cassandraUtils.dropIndex(TestingConstants.INDEX_NAME_CONSTANT)
                .waitForIndexRefresh()
                .dropTable()
                .waitForIndexRefresh()
                .dropKeyspace()
                .waitForIndexRefresh();

        System.out.println("tearDownSuite");
    }

    private String fromInteger(Integer[] list) {

        String out="{";
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
    public void biTemporalQueryIsWithInTimeStampFieldTest() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("is_within")
                .vtFrom("2014/12/31 12:00:00.000")
                .vtTo("2015/02/02 00:00:00.000")
                .ttFrom("2015/01/14 00:00:00.000")
                .ttTo("2015/02/16 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {1}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1}));
    }

    @Test
    public void biTemporalQueryIsWithInTimeStampFieldTest2() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("is_within")
                .vtFrom("2015/01/31 12:00:00.000")
                .vtTo("2015/03/02 00:00:00.000")
                .ttFrom("2015/02/14 00:00:00.000")
                .ttTo("2015/03/16 00:00:00.000"));



        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{2}));
    }

    @Test
    public void biTemporalQueryIsWithInTimeStampFieldTest3() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("is_within")
                .vtFrom("2015/02/28 12:00:00.000")
                .vtTo("2015/04/02 00:00:00.000")
                .ttFrom("2015/03/14 00:00:00.000")
                .ttTo("2015/04/16 00:00:00.000"));


        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{3}));
    }

    @Test
    public void biTemporalQueryIsWithInTimeStampFieldTest4() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("is_within")
                .vtFrom("2015/03/31 12:00:00.000")
                .vtTo("2015/05/02 00:00:00.000")
                .ttFrom("2015/04/14 00:00:00.000")
                .ttTo("2015/05/16 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {4}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{4}));
    }

    @Test
    public void biTemporalQueryIsWithInTimeStampFieldTes5() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("is_within")
                .vtFrom("2015/04/31 12:00:00.000")
                .vtTo("2015/06/02 00:00:00.000")
                .ttFrom("2015/05/14 00:00:00.000")
                .ttTo("2015/06/16 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {5}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{5}));
    }

    @Test
    public void biTemporalQueryIsWithInTimeStampFieldTes6() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("is_within")
                .vtFrom("2014/12/31 12:00:00.000")
                .vtTo("2015/03/02 00:00:00.000")
                .ttFrom("2015/01/14 00:00:00.000")
                .ttTo("2015/04/02 00:00:00.000"));

        assertEquals("Expected 2 results!", 2, select.count());
        assertTrue("Unexpected results!! Expected: {1,2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2}));
    }

    @Test
    public void biTemporalQueryIsWithInTimeStampFieldTes7() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("is_within")
                .vtFrom("2015/03/02 12:00:00.000")
                .vtTo("2015/03/10 00:00:00.000")
                .ttFrom("2015/01/14 00:00:00.000")
                .ttTo("2015/04/02 00:00:00.000"));

        assertEquals("Expected 0 results!", 0, select.count());
        assertTrue("Unexpected results!! Expected: {}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{}));
    }

    @Test
    public void biTemporalQueryIsWithInTimeStampFieldTes8() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("is_within")
                .vtFrom("2014/03/02 12:00:00.000")
                .vtTo("2015/03/10 00:00:00.000")
                .ttFrom("2015/01/14 00:00:00.000")
                .ttTo("2015/01/16 00:00:00.000"));

        assertEquals("Expected 0 results!", 0, select.count());
        assertTrue("Unexpected results!! Expected: {}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{}));
    }

    @Test
    public void biTemporalQueryIsWithInTimeStampFieldTest9() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("is_within")
                .vtFrom("2014/12/31 12:00:00.000")
                .vtTo("2015/06/02 00:00:00.000")
                .ttFrom("2015/01/14 00:00:00.000")
                .ttTo("2015/06/16 00:00:00.000"));

        assertEquals("Expected 5 results!", 5, select.count());
        assertTrue("Unexpected results!! Expected: {1,2,3,4,5}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2, 3, 4, 5}));
    }

    @Test
    public void biTemporalQueryContainsTimeStampFieldTest() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("contains")
                .vtFrom("2015/01/02 12:00:00.000")
                .vtTo("2015/01/30 00:00:00.000")
                .ttFrom("2015/01/16 00:00:00.000")
                .ttTo("2015/02/14 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {1}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1}));
    }


    @Test
    public void biTemporalQueryContainsTimeStampFieldTest2() {
        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("contains")
                .vtFrom("2015/02/02 12:00:00.000")
                .vtTo("2015/02/28 00:00:00.000")
                .ttFrom("2015/02/16 00:00:00.000")
                .ttTo("2015/03/14 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{2}));
    }

    @Test
    public void biTemporalQueryContainsTimeStampFieldTest3() {
        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("contains")
                .vtFrom("2015/03/02 12:00:00.000")
                .vtTo("2015/03/30 00:00:00.000")
                .ttFrom("2015/03/16 00:00:00.000")
                .ttTo("2015/04/14 00:00:00.000"));


        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{3}));
    }

    @Test
    public void biTemporalQueryContainsTimeStampFieldTest4() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("contains")
                .vtFrom("2015/04/02 12:00:00.000")
                .vtTo("2015/04/30 00:00:00.000")
                .ttFrom("2015/04/16 00:00:00.000")
                .ttTo("2015/05/14 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {4}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{4}));
    }

    @Test
    public void biTemporalQueryContainsTimeStampFieldTest5() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("contains")
                .vtFrom("2015/05/02 12:00:00.000")
                .vtTo("2015/05/30 00:00:00.000")
                .ttFrom("2015/05/16 00:00:00.000")
                .ttTo("2015/06/14 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {5}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{5}));
    }

    @Test
    public void biTemporalQueryContainsTimeStampFieldTest6() {
        //vt out of any example, tt inside first data, it must return 0

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("contains")
                .vtFrom("2014/10/31 12:00:00.000")
                .vtTo("2014/11/02 00:00:00.000")
                .ttFrom("2015/01/14 00:00:00.000")
                .ttTo("2015/06/16 00:00:00.000"));

        assertEquals("Expected 0 results!", 0, select.count());
        assertTrue("Unexpected results!! Expected: {}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{}));
    }

    @Test
    public void biTemporalQueryContainsTimeStampFieldTest7() {
        //vt inside a  data example, tt outside any data, it must return 0

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("contains")
                .vtFrom("2015/05/02 12:00:00.000")
                .vtTo("2015/05/30 00:00:00.000")
                .ttFrom("2014/05/16 00:00:00.000")
                .ttTo("2014/06/14 00:00:00.000"));

        assertEquals("Expected 0 results!", 0, select.count());
        assertTrue("Unexpected results!! Expected: {}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{}));
    }

    @Test
    public void biTemporalQueryIntersecsTimeStampFieldTest() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("intersects")
                .vtFrom("2015/01/01 00:00:00.000")
                .vtTo("2015/02/01 12:00:00.000")
                .ttFrom("2015/01/15 12:00:00.001")
                .ttTo("2015/02/15 12:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {1}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1}));
    }

    @Test
    public void biTemporalQueryIntersecsTimeStampFieldTest2() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("intersects")
                .vtFrom("2015/02/01 12:00:00.001")
                .vtTo("2015/03/01 12:00:00.000")
                .ttFrom("2015/02/15 12:00:00.001")
                .ttTo("2015/03/15 12:00:00.000"));


        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{2}));
    }

    @Test
    public void biTemporalQueryIntersecsTimeStampFieldTest3() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("intersects")
                .vtFrom("2015/03/01 12:00:00.001")
                .vtTo("2015/04/01 12:00:00.000")
                .ttFrom("2015/03/15 12:00:00.001")
                .ttTo("2015/04/15 12:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{3}));
    }

    @Test
    public void biTemporalQueryIntersecsTimeStampFieldTest4() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("intersects")
                .vtFrom("2015/04/01 12:00:00.001")
                .vtTo("2015/05/01 12:00:00.000")
                .ttFrom("2015/04/15 12:00:00.001")
                .ttTo("2015/05/15 12:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {4}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{4}));
    }

    @Test
    public void biTemporalQueryIntersecsTimeStampFieldTest5() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("intersects")
                .vtFrom("2015/05/01 12:00:00.001")
                .vtTo("2015/06/01 12:00:00.000")
                .ttFrom("2015/05/15 12:00:00.001")
                .ttTo("2015/06/15 12:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {5}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{5}));
    }

    @Test
    public void biTemporalQueryIntersecsTimeStampFieldTest6() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("intersects")
                .vtFrom("2014/12/31 12:00:00.000")
                .vtTo("2015/03/02 00:00:00.000")
                .ttFrom("2015/01/14 00:00:00.000")
                .ttTo("2015/04/02 00:00:00.000"));

        assertEquals("Expected 3 results!", 3, select.count());
        assertTrue("Unexpected results!! Expected: {1,2,3}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2, 3}));
    }

    @Test
    public void biTemporalQueryIntersecsTimeStampFieldTest7() {
        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("intersects")
                .vtFrom("2014/12/01 12:00:00.000")
                .vtTo("2014/12/31 00:00:00.000")
                .ttFrom("2015/01/14 00:00:00.000")
                .ttTo("2015/04/02 00:00:00.000"));

        assertEquals("Expected 0 results!", 0, select.count());
        assertTrue("Unexpected results!! Expected: {}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{}));
    }

    @Test
    public void biTemporalQueryIntersecsTimeStampFieldTest8() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("intersects")
                .vtFrom("2015/01/01 00:00:00.000")
                .vtTo("2015/02/01 12:00:00.001")
                .ttFrom("2015/01/15 12:00:00.001")
                .ttTo("2015/02/15 12:00:00.001"));

        assertEquals("Expected 2 results!", 2, select.count());
        assertTrue("Unexpected results!! Expected: {1,2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2}));
    }

    @Test
    public void biTemporalQueryIntersecsTimeStampFieldTest9() {

        CassandraUtilsSelect select=cassandraUtils.query(biTemporalSearch("bitemporal")
                .operation("intersects")
                .vtFrom("2015/02/01 12:00:00.000")
                .vtTo("2015/03/01 12:00:00.000")
                .ttFrom("2015/02/15 12:00:00.000")
                .ttTo("2015/03/15 12:00:00.000"));


        assertEquals("Expected 2 results!", 2, select.count());
        assertTrue("Unexpected results!! Expected: {1,2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2}));
    }
    private CassandraUtils setUpSuite2(String nowValue) {
        Map<String,String> fieldsMap= new HashMap<>();

        fieldsMap.put("bitemporal","{type:\"bitemporal\",tt_from:\"tt_from\", tt_to:\"tt_to\",vt_from:\"vt_from\", vt_to:\"vt_to\",pattern:\"yyyy/MM/dd HH:mm:ss.SSS\","+nowValue+"}");


        CassandraUtils cu=
                CassandraUtils.builder()
                        .withHost(TestingConstants.CASSANDRA_LOCALHOST_CONSTANT)
                        .withTable(TestingConstants.TABLE_NAME_CONSTANT)
                        .withIndexColumn(TestingConstants.INDEX_COLUMN_CONSTANT)
                        .withPartitionKey("integer_1")
                        .withClusteringKey()
                        .withColumn("integer_1", "int")
                        .withColumn("vt_from","text")
                        .withColumn("vt_to","text")
                        .withColumn("tt_from", "text")
                        .withColumn("tt_to", "text")
                        .withColumn("lucene", "text")
                        .build();


        cu.createKeyspace()
                .createTable()
                .createCustomIndex(TestingConstants.INDEX_NAME_CONSTANT, fieldsMap)
                .waitForIndexRefresh();

        return cu;
    }
    private CassandraUtils setUpSuite3() {
        Map<String,String> fieldsMap= new HashMap<>();

        fieldsMap.put("bitemporal","{type:\"bitemporal\",tt_from:\"tt_from\", tt_to:\"tt_to\",vt_from:\"vt_from\", vt_to:\"vt_to\"}");

        CassandraUtils cu=
                CassandraUtils.builder()
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

        cu.createKeyspace()
                .createTable()
                .createCustomIndex(TestingConstants.INDEX_NAME_CONSTANT, fieldsMap)
                .waitForIndexRefresh();

        return cu;


    }
    private void tearDown(CassandraUtils cu) {
        cu.dropIndex(TestingConstants.INDEX_NAME_CONSTANT)
                .waitForIndexRefresh()
                .dropTable()
                .waitForIndexRefresh()
                .dropKeyspace()
                .waitForIndexRefresh();

    }
    //valid long max value queries
    @Test
    public void biTemporalQueryIsWithInNowValueToLongTest() {
        //etstign with long value 1456876800 ==2016/03/02 00:00:00
        String nowValue="now_value:1456876800000";
        CassandraUtils cu=this.setUpSuite2(nowValue);
        cu.insert(DataHelper.data1)
                .insert(DataHelper.data2)
                .insert(DataHelper.data3)
                .insert(DataHelper.data4)
                .insert(DataHelper.data5)
                .waitForIndexRefresh();

        CassandraUtilsSelect select=cu.query(biTemporalSearch("bitemporal")
                .operation("is_within")
                .vtFrom("2015/02/28 12:00:00.000")
                .vtTo("2015/04/02 00:00:00.000")
                .ttFrom("2015/03/14 00:00:00.000")
                .ttTo("2015/04/16 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {3}, getted: "+fromInteger(select.intColumn("integer_1")),isThisAndOnlyThis(select.intColumn("integer_1"),new int[]{3}));
        tearDown(cu);
    }
    //inserting bigger to nowValue it must throw an IllegalArgumentException
    @Test(expected = WriteTimeoutException.class)
    public void biTemporalQueryIsWithInNowValueToLongTest2() {
        //etstign with long value 1456876800 ==2016/03/02 00:00:00
        String nowValue="now_value:1456876800000";
        CassandraUtils cu=this.setUpSuite2(nowValue);


        cu.insert(DataHelper.data6).waitForIndexRefresh();

        //tearDown(cu);
    }
    //querying bigger than nowValue it must throw an IllegalArgumentException
    @Test(expected = InvalidQueryException.class)
    public void biTemporalQueryIsWithInNowValueToLongTest3() {
        //etstign with long value 1456876800 ==2016/03/02 00:00:00
        String nowValue="now_value:1456876800000";
        CassandraUtils cu=this.setUpSuite2(nowValue);
        cu.insert(DataHelper.data1);
        cu.insert(DataHelper.data2);
        cu.insert(DataHelper.data3);
        cu.insert(DataHelper.data4);
        cu.insert(DataHelper.data5);
        cu.waitForIndexRefresh();
        cu.query(biTemporalSearch("bitemporal")
                .operation("is_within")
                .vtFrom("2016/02/28 12:00:00.000")
                .vtTo("2016/04/02 00:00:00.000")
                .ttFrom("2016/03/14 00:00:00.000")
                .ttTo("2016/04/16 00:00:00.000")).count();
        tearDown(cu);
    }

    //valid String max value queries
    @Test
    public void biTemporalQueryIsWithInNowValueToStringTest() {
        //testing with string value
        String nowValue = "now_value:\"2016/03/02 00:00:00.000\"";
        CassandraUtils cu=this.setUpSuite2(nowValue)
                .insert(DataHelper.data1)
                .insert(DataHelper.data2)
                .insert(DataHelper.data3)
                .insert(DataHelper.data4)
                .insert(DataHelper.data5)
                .waitForIndexRefresh();
        //testing if inserting data translate it to Long.max
        CassandraUtilsSelect select=cu.query(biTemporalSearch("bitemporal")
                .operation("is_within")
                .vtFrom("2015/02/28 12:00:00.000")
                .vtTo("2015/04/02 00:00:00.000")
                .ttFrom("2015/03/14 00:00:00.000")
                .ttTo("2015/04/16 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {3}, getted: "+fromInteger(select.intColumn("integer_1")),isThisAndOnlyThis(select.intColumn("integer_1"),new int[]{3}));
        tearDown(cu);
    }
    //inserting bigger to nowValue it must throw an IllegalArgumentException
    @Test(expected = WriteTimeoutException.class)
    public void biTemporalQueryIsWithInNowValueToStringTest2() {
        //etstign with long value
        String nowValue="now_value:\"2016/03/02 00:00:00.000\"";
        CassandraUtils cu=this.setUpSuite2(nowValue);


        //testing if inserting data translate it to Long.max
        cu.insert(DataHelper.data6).waitForIndexRefresh();

        tearDown(cu);
        //testing if querying data translate it to Long.max


        tearDown(cu);
    }
    //querying bigger than nowValue it must throw an IllegalArgumentException
    @Test(expected = InvalidQueryException.class)
    public void biTemporalQueryIsWithInNowValueToStringTest3() {
        //etstign with long value
        String nowValue="now_value:\"2016/03/02 00:00:00.000\"";
        CassandraUtils cu=this.setUpSuite2(nowValue)
                .insert(DataHelper.data1)
                .insert(DataHelper.data2)
                .insert(DataHelper.data3)
                .insert(DataHelper.data4)
                .insert(DataHelper.data5)
                .waitForIndexRefresh();
        cu.query(biTemporalSearch("bitemporal")
                .operation("is_within")
                .vtFrom("2016/02/28 12:00:00.000")
                .vtTo("2016/04/02 00:00:00.000")
                .ttFrom("2016/03/14 00:00:00.000")
                .ttTo("2016/04/16 00:00:00.000")).count();
        //testing if inserting data translate it to Long.max

        tearDown(cu);
    }
    //valid String max value queries settign nowValue to max date in data3
    @Test
    public void biTemporalQueryIsWithInNowValueToStringTest4() {
        //testing with string value
        String nowValue = "now_value:\"2015/04/15 12:00:00.000\"";
        CassandraUtils cu=this.setUpSuite2(nowValue)
                .insert(DataHelper.data1)
                .insert(DataHelper.data2)
                .insert(DataHelper.data3)
                .waitForIndexRefresh();
        //testing if inserting data translate it to Long.max

        CassandraUtilsSelect select=cu.query(biTemporalSearch("bitemporal")
            .operation("intersects")
            .vtFrom("2014/12/31 12:00:00.000")
            .vtTo("2015/03/02 00:00:00.000")
            .ttFrom("2015/01/14 00:00:00.000")
            .ttTo("2015/04/02 00:00:00.000"));

        assertEquals("Expected 3 results!", 3, select.count());
        assertTrue("Unexpected results!! Expected: {1,2,3}, getted: "+fromInteger(select.intColumn("integer_1")),isThisAndOnlyThis(select.intColumn("integer_1"),new int[]{1,2,3}));
    }

    //quering without limits to vt
    @Test
    public void biTemporalQueryIsWithInNowValueToStringTest5() {
        //testing with string value
        String nowValue = "now_value:\"2015/04/15 12:00:00.000\"";
        CassandraUtils cu=this.setUpSuite2(nowValue)
                .insert(DataHelper.data1)
                .insert(DataHelper.data2)
                .insert(DataHelper.data3)
                .waitForIndexRefresh();
        //testing if inserting data translate it to Long.max

        CassandraUtilsSelect select=cu.query(biTemporalSearch("bitemporal")
                .operation("intersects")
                .ttFrom("2015/01/14 00:00:00.000")
                .ttTo("2015/04/02 00:00:00.000"));

        assertEquals("Expected 3 results!", 3, select.count());
        assertTrue("Unexpected results!! Expected: {1,2,3}, getted: "+fromInteger(select.intColumn("integer_1")),isThisAndOnlyThis(select.intColumn("integer_1"),new int[]{1,2,3}));
    }
    //quering without limits to tt
    @Test
    public void biTemporalQueryIsWithInNowValueToStringTest6() {
        //testing with string value
        String nowValue = "now_value:\"2015/04/15 12:00:00.000\"";
        CassandraUtils cu=this.setUpSuite2(nowValue)
                .insert(DataHelper.data1)
                .insert(DataHelper.data2)
                .insert(DataHelper.data3)
                .waitForIndexRefresh();
        //testing if inserting data translate it to Long.max

        CassandraUtilsSelect select=cu.query(biTemporalSearch("bitemporal")
                .operation("intersects")
                .vtFrom("2014/12/31 12:00:00.000")
                .vtTo("2015/03/02 00:00:00.000"));


        assertEquals("Expected 3 results!", 3, select.count());
        assertTrue("Unexpected results!! Expected: {1,2,3}, getted: "+fromInteger(select.intColumn("integer_1")),isThisAndOnlyThis(select.intColumn("integer_1"),new int[]{1,2,3}));
    }



    @Test
    public void biTemporalQueryOverBigIntsWithDefaultPattern() {
        CassandraUtils cu=this.setUpSuite3()
                .insert(DataHelper.data7)
                .insert(DataHelper.data8)
                .insert(DataHelper.data9)
                .insert(DataHelper.data10)
                .insert(DataHelper.data11)
                .waitForIndexRefresh();


        CassandraUtilsSelect select=cu.searchAll();


        assertEquals("Expected 5 results!", 5, select.count());
        assertTrue("Unexpected results!! Expected: {1,2,3,4,5}, getted: "+fromInteger(select.intColumn("id")),isThisAndOnlyThis(select.intColumn("id"),new int[]{1,2,3,4,5}));



        Batch batch= QueryBuilder.batch();
        Update update = QueryBuilder.update(cu.getKeyspace(), cu.getTable());

        update.where(QueryBuilder.eq("id", 1))
                    .and(QueryBuilder.eq("vt_from",0))
                    .and(QueryBuilder.eq("tt_from",0))
                .onlyIf(QueryBuilder.eq("tt_to",9223372036854775807l))
                .with(QueryBuilder.set("tt_to", 20150101));

        batch.add(update);

        Insert insert =  QueryBuilder.insertInto(cu.getKeyspace(),cu.getTable());
        insert.value("id",1)
                .value("data","v2")
                .value("vt_from",0)
                .value("vt_to", 9223372036854775807l)
                .value("tt_from",20150102)
                .value("tt_to",9223372036854775807l);

        batch.add(insert);
        ResultSet result = cu.getSession().execute(batch);


        assertTrue("batch execution didnt worked",result.wasApplied());
        cu.waitForIndexRefresh();


        CassandraUtilsSelect select2=cu.filter(biTemporalSearch("bitemporal")
                .operation("intersects")
                .vtFrom(0)
                .vtTo(9223372036854775807l)
                .ttFrom(9223372036854775807l)
                .ttTo(9223372036854775807l));


        assertEquals("Expected 5 results!", 5, select2.count());
        assertTrue("Unexpected results!! Expected: {1,2,3,4,5}, getted: "+fromInteger(select2.intColumn("id")),isThisAndOnlyThis(select2.intColumn("id"),new int[]{1,2,3,4,5}));



        CassandraUtilsSelect select3=cu.filter(biTemporalSearch("bitemporal")
                .operation("intersects")
                .vtFrom(0)
                .vtTo(9223372036854775807l)
                .ttFrom(9223372036854775807l)
                .ttTo(9223372036854775807l)).and("AND id = 1");

        assertEquals("Expected 1 results!", 1, select3.count());
        Row row=select3.get().get(0);

        assertTrue("Unexpected results!! Expected result : {id=\"1\"}, getted: "+row.getInt("id"),row.getInt("id")==1);
        assertTrue("Unexpected results!! Expected result : {data=\"v2\"}, getted: "+row.getString("data"),row.getString("data").equals("v2"));
        assertTrue("Unexpected results!! Expected result : {vt_from=0}, getted: "+row.getLong("vt_from"),row.getLong("vt_from")==0l);
        assertTrue("Unexpected results!! Expected result : {vt_to=0}, getted: "+row.getLong("vt_to"),row.getLong("vt_to")==9223372036854775807l);
        assertTrue("Unexpected results!! Expected result : {tt_from=0}, getted: "+row.getLong("tt_from"),row.getLong("tt_from")==20150102l);
        assertTrue("Unexpected results!! Expected result : {tt_to=0}, getted: "+row.getLong("tt_to"),row.getLong("tt_to")==9223372036854775807l);

    }
}
