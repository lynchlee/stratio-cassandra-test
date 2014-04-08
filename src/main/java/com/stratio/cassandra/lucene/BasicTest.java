package com.stratio.cassandra.lucene;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

@RunWith(JUnit4.class)
public class BasicTest {

    private static final Logger logger = Logger.getLogger(BasicTest.class);

    private static final String CASSANDRA_HOST_CONSTANT = "127.0.0.1";

    private static final String KEYSPACE_CONSTANT = "indexes";

    private static final int REPLICATION_FACTOR_CONSTANT = 2;

    private static final String TABLE_NAME_CONSTANT = "testIndexes";

    private static final String INDEX_NAME_CONSTANT = "indexIndexes";

    private static final String INDEX_COLUMN_CONSTANT = "lucene";

    private static Cluster cluster;

    @BeforeClass
    public static void setUpTests() {

        // DB Connection
        logger.debug("Connecting to database");
        cluster = Cluster.builder().addContactPoint(CASSANDRA_HOST_CONSTANT)
                .build();
        Session session = cluster.connect();

        // Defining data model
        logger.debug("Defining db table");
        Map<String, String> columns = new Hashtable<String, String>();
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
        columns.put("lucene", "text");

        logger.debug("Defining table primary key");
        Map<String, List<String>> primaryKey = new Hashtable<String, List<String>>();
        String[] inarray = { "integer_1" };
        String[] outarray = {};
        List<String> in = Arrays.asList(inarray);
        List<String> out = Arrays.asList(outarray);
        primaryKey.put("in", in);
        primaryKey.put("out", out);

        // Creating keyspace
        logger.debug("Creating db keyspace");
        String query = "create keyspace "
                + KEYSPACE_CONSTANT
                + " with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : '"
                + REPLICATION_FACTOR_CONSTANT + "' };";
        session.execute(query);

        // Creating table
        logger.debug("Creating db table");
        String tableQuery = createTableQuery(columns, primaryKey,
                KEYSPACE_CONSTANT, TABLE_NAME_CONSTANT);
        session.execute(tableQuery);

        logger.debug("Creating db secondary index");
        createIndexQuery(columns);
    }

    private static String createTableQuery(Map<String, String> columns,
            Map<String, List<String>> primaryKey, String keyspace,
            String tableName) {
        String columnDefinition = "";
        // Convert HashTable of columns to string
        for (String s : columns.keySet()) {
            columnDefinition = columnDefinition + s + " " + columns.get(s)
                    + ",";
        }
        // Create de primary key
        String primaryKeyString = createPrimaryKey(primaryKey);
        String query = "CREATE TABLE " + keyspace + "." + tableName + " ("
                + columnDefinition + primaryKeyString + ");";
        logger.debug("Table: " + query);
        return query;
    }

    private static String createPrimaryKey(Map<String, List<String>> primaryKey) {
        String in = "";
        String out = "";
        if (primaryKey.isEmpty())
            return "";
        if (primaryKey.containsKey("in")) {
            for (String s : primaryKey.get("in")) {
                in = in + s + ",";
            }
        }
        if (primaryKey.containsKey("out")) {
            for (String s : primaryKey.get("out")) {
                out = out + s + ",";
            }
        }

        String res = "PRIMARY KEY ";
        if (out != "") {
            out = out.substring(0, out.length() - 1);
            out = "," + out + ")";
            res = res + "(";
        } else if (in != "") {
            in = in.substring(0, in.length() - 1);
            in = "(" + in + ")";
        }
        String result = res + in + out;
        return result;

    }

    private static String createIndexQuery(Map<String, String> columns) {
        String indexStart = "CREATE CUSTOM INDEX "
                + INDEX_NAME_CONSTANT
                + " ON "
                + KEYSPACE_CONSTANT
                + "."
                + TABLE_NAME_CONSTANT
                + " ("
                + INDEX_COLUMN_CONSTANT
                + ") USING 'org.apache.cassandra.db.index.stratio.RowIndex' WITH OPTIONS = {"
                + "'refresh_seconds':'1',"
                + "'num_cached_filters':'1',"
                + "'ram_buffer_mb':'64',"
                + "'max_merge_mb':'5',"
                + "'max_cached_mb':'30',"
                + " 'schema':'{default_analyzer:\"org.apache.lucene.analysis.standard.StandardAnalyzer\",fields:{";
        String indexMed = conversionCassToLucene(columns);

        String indexEnd = "}}'};";
        String query = indexStart + indexMed + indexEnd;
        logger.debug("Index: " + query);
        return query;
    }

    private static String conversionCassToLucene(Map<String, String> columns) {
        // http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/cql_data_types_c.html
        Hashtable<String, String> conversionCassandraLucene = new Hashtable<String, String>();
        conversionCassandraLucene.put("ascii", ":{type:\"string\"}");
        conversionCassandraLucene.put("bigint", ":{type:\"long\"}");
        conversionCassandraLucene.put("blob", ":{type:\"bytes\"}");
        conversionCassandraLucene.put("boolean", ":{type:\"boolean\"}");
        conversionCassandraLucene.put("counter", ":{type:\"long\"}");
        conversionCassandraLucene.put("decimal",
                ":{type:\"bigdec\", integer_digits:10, decimal_digits:10}");
        conversionCassandraLucene.put("double", ":{type:\"double\"}");
        conversionCassandraLucene.put("float", ":{type:\"float\"}");
        conversionCassandraLucene.put("inet", ":{type:\"string\"}");
        conversionCassandraLucene.put("int", ":{type:\"integer\"}");
        conversionCassandraLucene.put("text", ":{type:\"string\"}");
        conversionCassandraLucene.put("timestamp",
                ":{type:\"date\", pattern: \"yyyy/MM/dd\"}");
        conversionCassandraLucene.put("uuid", ":{type:\"uuid\"}");
        conversionCassandraLucene.put("timeuuid", ":{type:\"uuid\"}");
        conversionCassandraLucene.put("varchar", ":{type:\"string\"}");
        conversionCassandraLucene
                .put("varint", ":{type:\"bigint\", digits:10}");
        String converted = "";
        for (String s : columns.keySet()) {
            if (!columns.get(s).endsWith(">")) {
                String type = columns.get(s);
                String lucenetype = conversionCassandraLucene.get(type);
                converted = converted + s + lucenetype + ",";
            } else {
                String typeComp = columns.get(s);
                int indexTypeStart = typeComp.indexOf("<");
                int indexTypeEnd = typeComp.indexOf(">");
                String type = typeComp.substring(indexTypeStart, indexTypeEnd);
                String lucenetype = conversionCassandraLucene.get(type);
                converted = converted + s + lucenetype + ",";
            }
        }
        return converted.substring(0, converted.length() - 1);
    }

    @AfterClass
    public static void tearDownTests() {
        // DB Connection
        logger.debug("Connecting to database");
        cluster = Cluster.builder().addContactPoint(CASSANDRA_HOST_CONSTANT)
                .build();
        Session session = cluster.connect();

        String query = "drop keyspace " + KEYSPACE_CONSTANT + " ;";
        session.execute(query);

        session.close();

    }

    @Test()
    public void DummyTest() {

        logger.debug("Dummy test");
    }
}