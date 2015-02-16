package com.stratio.cassandra.lucene.util;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.stratio.cassandra.index.query.builder.ConditionBuilder;
import com.stratio.cassandra.index.query.builder.SortFieldBuilder;
import com.stratio.cassandra.lucene.TestingConstants;
import org.apache.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CassandraUtils {

    private static final Logger logger = Logger.getLogger(CassandraUtils.class);

    private static final Map<String, String> conversionCassandraLucene;

    static {
        // http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/cql_data_types_c.html
        conversionCassandraLucene = new LinkedHashMap<>();
        conversionCassandraLucene.put("ascii", ":{type:\"string\"}");
        conversionCassandraLucene.put("bigint", ":{type:\"long\"}");
        conversionCassandraLucene.put("blob", ":{type:\"bytes\"}");
        conversionCassandraLucene.put("boolean", ":{type:\"boolean\"}");
        conversionCassandraLucene.put("counter", ":{type:\"long\"}");
        conversionCassandraLucene.put("decimal", ":{type:\"bigdec\", integer_digits:10, decimal_digits:10}");
        conversionCassandraLucene.put("double", ":{type:\"double\"}");
        conversionCassandraLucene.put("float", ":{type:\"float\"}");
        conversionCassandraLucene.put("inet", ":{type:\"inet\"}");
        conversionCassandraLucene.put("int", ":{type:\"integer\"}");
        conversionCassandraLucene.put("text", ":{type:\"text\"}");
        conversionCassandraLucene.put("timestamp", ":{type:\"date\", pattern: \"yyyy/MM/dd\"}");
        conversionCassandraLucene.put("uuid", ":{type:\"uuid\"}");
        conversionCassandraLucene.put("timeuuid", ":{type:\"uuid\"}");
        conversionCassandraLucene.put("varchar", ":{type:\"string\"}");
        conversionCassandraLucene.put("varint", ":{type:\"bigint\", digits:10}");
    }

    private final Cluster cluster;
    private final String host;
    private Metadata metadata;
    private Session session;
    private ConsistencyLevel consistencyLevel;
    private final String keyspace;
    private final String table;
    private final String qualifiedTable;
    private final Map<String, String> columns;
    private final List<String> partitionKey;
    private final List<String> clusteringKey;
    private final String indexColumn;
    private final String columnsWithoutLucene;
    private String replicationFactor;

    public static CassandraUtilsBuilder builder() {
        return new CassandraUtilsBuilder();
    }

    public CassandraUtils(String host,
                          String keyspace,
                          String table,
                          Map<String, String> columns,
                          List<String> partitionKey,
                          List<String> clusteringKey,
                          String indexColumn) {

        String consistencyLevelString = System.getProperty(TestingConstants.CONSISTENCY_LEVEL_CONSTANT_NAME);

        if (consistencyLevelString == null) consistencyLevelString = TestingConstants.DEFAULT_CONSISTENCY_LEVEL;

        consistencyLevel = ConsistencyLevel.valueOf(consistencyLevelString);

        this.host = host;
        this.cluster = Cluster.builder().addContactPoint(host).build();
        this.cluster.getConfiguration().getQueryOptions().setConsistencyLevel(consistencyLevel);
        this.cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(600000);
        metadata = cluster.getMetadata();
        logger.debug("Connected to cluster (" + this.host + "): " + metadata.getClusterName() + "\n");
        session = cluster.connect();

        String replicationFactorString = System.getProperty(TestingConstants.REPLICATION_FACTOR_CONSTANT_NAME);

        if (replicationFactorString == null || Integer.parseInt(replicationFactorString) < 1)
            replicationFactorString = "1";

        this.replicationFactor = replicationFactorString;

        this.keyspace = keyspace;
        this.table = table;
        this.columns = columns;
        this.partitionKey = partitionKey;
        this.clusteringKey = clusteringKey;
        this.indexColumn = indexColumn;
        this.columnsWithoutLucene = columnsWithoutLucene();
        qualifiedTable = keyspace + "." + table;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getTable() {
        return table;
    }

    public String getIndexColumn() {
        return indexColumn;
    }

    private String columnsWithoutLucene() {

        String result = "";
        for (String s : columns.keySet()) {
            if (!s.equals(indexColumn)) {
                result = result + s + ",";
            }
        }
        result = result.substring(0, result.length() - 1);

        return result;
    }

    protected List<Row> execute(StringBuilder sb) {
        return execute(sb.toString());
    }

    protected List<Row> execute(Statement statement) {
        return execute(statement.toString());
    }

    protected List<Row> execute(String query) {
        if (!query.endsWith(";")) query += ";";
        logger.debug("CQL: " + query);
        if (TestingConstants.READ_WAIT_TIME > 0) {
            try {
                Thread.sleep(TestingConstants.READ_WAIT_TIME);
            } catch (InterruptedException e) {
                logger.error("Interruption caught during a Thread.sleep; index might be unstable");
            }
        }
        return session.execute(query).all();
    }

    public void disconnect() {
        session.close();
    }

    public CassandraUtils waitForIndexRefresh() {

        // Waiting for the custom index to be refreshed
        logger.debug("Waiting for the index to be refreshed...");
        try {
            Thread.sleep(TestingConstants.WRITE_WAIT_TIME);
        } catch (InterruptedException e) {
            logger.error("Interruption caught during a Thread.sleep; index might be unstable");
        }
        logger.debug("Index ready to rock!");

        return this;
    }

    public CassandraUtils createKeyspace() {
        execute(new StringBuilder().append("CREATE KEYSPACE ")
                                   .append(keyspace)
                                   .append(" with replication = ")
                                   .append("{'class' : 'SimpleStrategy', 'replication_factor' : '")
                                   .append(replicationFactor)
                                   .append("' };"));
        return this;
    }

    public CassandraUtils dropTable() {
        execute("DROP TABLE " + qualifiedTable);
        return this;
    }

    public CassandraUtils dropKeyspace() {
        execute("DROP KEYSPACE " + keyspace + " ;");
        return this;
    }

    public CassandraUtils createTable() {
        StringBuilder sb = new StringBuilder().append("CREATE TABLE ").append(qualifiedTable).append(" (");
        for (String s : columns.keySet()) {
            sb.append(s).append(" ").append(columns.get(s)).append(", ");
        }
        sb.append("PRIMARY KEY ((");
        for (int i = 0; i < partitionKey.size(); i++) {
            sb.append(partitionKey.get(i));
            sb.append(i == partitionKey.size() - 1 ? ")" : ",");
        }
        for (String s : clusteringKey) {
            sb.append(", ").append(s);
        }
        sb.append("))");
        execute(sb);
        return this;
    }

    public CassandraUtils truncateTable() {
        execute(new StringBuilder().append("TRUNCATE ").append(qualifiedTable));
        return this;
    }

    public CassandraUtils createIndex(String indexName) {
        execute(new StringBuilder().append("CREATE CUSTOM INDEX ")
                                   .append(indexName)
                                   .append(" ON ")
                                   .append(qualifiedTable)
                                   .append(" (")
                                   .append(indexColumn)
                                   .append(") USING 'org.apache.cassandra.db.index.stratio.RowIndex' WITH OPTIONS = {")
                                   .append("'refresh_seconds':'0.1',")
                                   .append("'num_cached_filters':'1',")
                                   .append("'ram_buffer_mb':'64',")
                                   .append("'max_merge_mb':'5',")
                                   .append("'max_cached_mb':'30',")
                                   .append(" 'schema':'{default_analyzer:")
                                   .append("\"org.apache.lucene.analysis.standard.StandardAnalyzer\",fields:{")
                                   .append(conversionCassToLucene())
                                   .append("}}'};"));

        return this;
    }

    public CassandraUtils dropIndex(String indexName) {
        execute(new StringBuilder().append("DROP INDEX ").append(keyspace).append(".").append(indexName).append(";"));
        return this;
    }

    private String conversionCassToLucene() {
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
                String type = typeComp.substring(indexTypeStart + 1, indexTypeEnd);
                if (type.contains(",")) {
                    type = type.substring(0, type.indexOf(","));
                }

                String lucenetype = conversionCassandraLucene.get(type);
                converted = converted + s + lucenetype + ",";
            }
        }
        return converted.substring(0, converted.length() - 1);
    }

    public List<Row> selectAllFromTable() {
        return execute(new StringBuilder().append("SELECT ")
                                          .append(columnsWithoutLucene)
                                          .append(" FROM ")
                                          .append(qualifiedTable));
    }

    public List<Row> selectAllFromIndexQueryWithFiltering(int limit, String name, Object value) {
        return execute(QueryBuilder.select()
                                   .from(keyspace, table)
                                   .where(QueryBuilder.eq(indexColumn, "{}"))
                                   .and(QueryBuilder.eq(name, value))
                                   .limit(limit)
                                   .allowFiltering()
                                   .setConsistencyLevel(consistencyLevel));
    }

    public CassandraUtils insert(Map<String, String> params) {

        String columns = "";
        String values = "";
        for (String s : params.keySet()) {
            if (!s.equals(indexColumn)) {
                columns += s + ",";
                values = values + params.get(s) + ",";
            }
        }
        columns = columns.substring(0, columns.length() - 1);
        values = values.substring(0, values.length() - 1);

        execute(new StringBuilder().append("INSERT INTO ")
                                   .append(qualifiedTable)
                                   .append(" (")
                                   .append(columns)
                                   .append(") VALUES (")
                                   .append(values)
                                   .append(");"));

        return this;
    }

    public CassandraUtils deleteValueByCondition(String field, String condition) {
        execute(new StringBuilder().append("DELETE ")
                                   .append(field)
                                   .append(" FROM ")
                                   .append(qualifiedTable)
                                   .append(" WHERE ")
                                   .append(condition));
        return this;
    }

    public CassandraUtils deleteByCondition(String condition) {
        execute(new StringBuilder().append("DELETE ")
                                   .append(" FROM ")
                                   .append(qualifiedTable)
                                   .append(" WHERE ")
                                   .append(condition));
        return this;
    }

    public CassandraUtilsUpdate update() {
        return new CassandraUtilsUpdate(this);
    }

    public CassandraUtilsSelect select() {
        return new CassandraUtilsSelect(this);
    }

    public CassandraUtilsSelect searchAll() {
        return new CassandraUtilsSelect(this).search();
    }

    public CassandraUtilsSelect query(ConditionBuilder<?, ?> query) {
        return new CassandraUtilsSelect(this).query(query);
    }

    public CassandraUtilsSelect filter(ConditionBuilder<?, ?> filter) {
        return new CassandraUtilsSelect(this).filter(filter);
    }

    public CassandraUtilsSelect sort(SortFieldBuilder... sort) {
        return new CassandraUtilsSelect(this).sort(sort);
    }

}
