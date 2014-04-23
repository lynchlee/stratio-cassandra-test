package com.stratio.cassandra.lucene.breakdown;

/**
 * Created by Jcalderin on 24/03/14.
 */

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.QueryUtils;

@RunWith(JUnit4.class)
public class BreakdownTest {

    private static final Logger logger = Logger.getLogger(BreakdownTest.class);

    private static QueryUtils queryUtils;

    private static CassandraUtils cassandraUtils;

    @BeforeClass
    public static void setUpSuite() {

        // Initializing suite data
        Map<String, String> columns = new LinkedHashMap<String, String>();
        columns.put("ascii_1", "ascii");
        columns.put("bigint_1", "bigint");
        columns.put("blob_1", "blob");
        columns.put("boolean_1", "boolean");
        columns.put("decimal_1", "decimal");
        columns.put("date_1", "timestamp");
        columns.put("double_1", "double");
        columns.put("float_1", "float");
        columns.put("integer_1", "int");
        columns.put("inet_1", "inet");
        columns.put("text_1", "text");
        columns.put("varchar_1", "varchar");
        columns.put("uuid_1", "uuid");
        columns.put("timeuuid_1", "timeuuid");
        columns.put("list_1", "list<text>");
        columns.put("set_1", "set<text>");
        columns.put("map_1", "map<text,text>");
        columns.put("lucene", "text");

        Map<String, List<String>> primaryKey = new LinkedHashMap<String, List<String>>();
        String[] inarray = { "integer_1" };
        String[] outarray = {};
        List<String> in = Arrays.asList(inarray);
        List<String> out = Arrays.asList(outarray);
        primaryKey.put("in", in);
        primaryKey.put("out", out);

        queryUtils = new QueryUtils(TestingConstants.KEYSPACE_CONSTANT,
                TestingConstants.TABLE_NAME_CONSTANT, columns, primaryKey,
                TestingConstants.INDEX_COLUMN_CONSTANT);

        cassandraUtils = new CassandraUtils(
                TestingConstants.CASSANDRA_LOCALHOST_CONSTANT);

        // Executing db queries
        List<String> queriesList = new ArrayList<>();

        String keyspaceCreationQuery = queryUtils.createKeyspaceQuery();
        String tableCreationQuery = queryUtils.createTableQuery();
        String indexCreationQuery = queryUtils
                .createIndex(TestingConstants.INDEX_NAME_CONSTANT);

        queriesList.add(keyspaceCreationQuery);
        queriesList.add(tableCreationQuery);
        queriesList.add(indexCreationQuery);
        queriesList.add(queryUtils.getInsert(QueryTypeDataHelper.data1));
        queriesList.add(queryUtils.getInsert(QueryTypeDataHelper.data2));
        queriesList.add(queryUtils.getInsert(QueryTypeDataHelper.data3));
        queriesList.add(queryUtils.getInsert(QueryTypeDataHelper.data4));

        cassandraUtils.executeQueriesList(queriesList, true);
    };

    @AfterClass
    public static void tearDownSuite() {

        logger.debug("Dropping keyspace");
        cassandraUtils.executeQuery(queryUtils.dropKeyspaceQuery());

        cassandraUtils.disconnect();
    };

    @Test
    public void breakdownTest1() {

        Map<String, String> data4Modified = new LinkedHashMap<>(
                QueryTypeDataHelper.data4);
        data4Modified.put("double_1", "40");

        // TODO Kill node2
        Process p;
        try {
            logger.info("Stopping the node1 before modifying data...");
            p = Runtime.getRuntime().exec("ccm node1 stop");
            p.waitFor();
            logger.info("Node1 stopped!");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        cassandraUtils.executeQuery(queryUtils.getInsert(data4Modified), true);
        logger.info("Data modified");

        // TODO Recover node2
        try {
            logger.info("Starting the node1...");
            p = Runtime.getRuntime().exec("ccm node1 start");
            p.waitFor();
            logger.info("Node1 started, let's repair it...");
            p = Runtime.getRuntime().exec("ccm node1 repair");
            p.waitFor();
            logger.info("Node1 repaired!");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        ResultSet resultSet = cassandraUtils.executeQuery(queryUtils
                .selectAllFromTable());
        List<Row> rows = resultSet.all();

        boolean modified = false;
        for (int i = 0; i < rows.size(); i++) {
            double rowDouble1 = rows.get(i).getDouble("double_1");
            if (rowDouble1 == 40) {
                modified = true;
            }
        }

        assertTrue("Value wasn't modified!", modified);
    }
}
