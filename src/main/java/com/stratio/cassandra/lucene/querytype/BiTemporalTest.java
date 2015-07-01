package com.stratio.cassandra.lucene.querytype;

import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.CassandraUtilsSelect;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.search.SearchBuilders.biTemporalSearch;
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

   /* @AfterClass
    public static void tearDownSuite() {
        cassandraUtils.dropIndex(TestingConstants.INDEX_NAME_CONSTANT)
                .waitForIndexRefresh()
                .dropTable()
                .waitForIndexRefresh()
                .dropKeyspace()
                .waitForIndexRefresh();
    }*/

    private String fromInteger(Integer[] list) {
        String out = "{";
        for (Integer i : list) {
            out += Integer.toString(i) + ",";
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("is_within")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("is_within")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("is_within")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("is_within")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("is_within")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("is_within")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("is_within")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("is_within")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("is_within")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("contains")
                                                                                         .vtFrom("2015/01/02 12:00:00.000")
                                                                                         .vtTo("2015/01/30 00:00:00.000")
                                                                                         .ttFrom("2015/01/16 00:00:00.000")
                                                                                         .ttTo("2015/02/14 00:00:00.000"));

        assertEquals("Expected 1 results!", 1, select.count());
        assertTrue("Unexpected results!! Expected: {1}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1}));
    }

    //TODO
    @Test
    public void biTemporalQueryContainsTimeStampFieldTest2() {
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("contains")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("contains")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("contains")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("contains")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("contains")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("contains")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("intersects")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("intersects")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("intersects")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("intersects")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("intersects")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("intersects")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("intersects")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("intersects")
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
        CassandraUtilsSelect select = cassandraUtils.query(biTemporalSearch("bitemporal").operation("intersects")
                                                                                         .vtFrom("2015/02/01 12:00:00.000")
                                                                                         .vtTo("2015/03/01 12:00:00.000")
                                                                                         .ttFrom("2015/02/15 12:00:00.000")
                                                                                         .ttTo("2015/03/15 12:00:00.000"));

        assertEquals("Expected 2 results!", 2, select.count());
        assertTrue("Unexpected results!! Expected: {1,2}, got: " + fromInteger(select.intColumn("integer_1")),
                   isThisAndOnlyThis(select.intColumn("integer_1"), new int[]{1, 2}));
    }

}
