package com.stratio.cassandra.lucene.util;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.schema.SchemaBuilder;
import com.stratio.cassandra.lucene.search.condition.builder.ConditionBuilder;
import com.stratio.cassandra.lucene.search.sort.builder.SortFieldBuilder;
import org.apache.log4j.Logger;


import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.stratio.cassandra.lucene.TestingConstants.*;
import static com.stratio.cassandra.lucene.search.SearchBuilders.all;
import static com.stratio.cassandra.lucene.search.SearchBuilders.search;

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

    public  CassandraUtils(String host,
                          String keyspace,
                          String table,
                          Map<String, String> columns,
                          List<String> partitionKey,
                          List<String> clusteringKey,
                          String indexColumn) {

        String consistencyLevelString = System.getProperty(CONSISTENCY_LEVEL_CONSTANT_NAME);

        if (consistencyLevelString == null) consistencyLevelString = DEFAULT_CONSISTENCY_LEVEL;

        consistencyLevel = ConsistencyLevel.valueOf(consistencyLevelString);

        this.host = host;
        this.cluster = Cluster.builder().addContactPoint(host).build();
        this.cluster.getConfiguration().getQueryOptions().setConsistencyLevel(consistencyLevel);
        this.cluster.getConfiguration().getQueryOptions().setFetchSize(Integer.MAX_VALUE);
        this.cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(600000);
        metadata = cluster.getMetadata();
        logger.debug("Connected to cluster (" + this.host + "): " + metadata.getClusterName() + "\n");
        session = cluster.connect();

        String replicationFactorString = System.getProperty(TestingConstants.REPLICATION_FACTOR_CONSTANT_NAME);

        if (replicationFactorString == null || Integer.parseInt(replicationFactorString) < 1)
            replicationFactorString = DEFAULT_REPLICATION_FACTOR;

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
        if (result.length()>0){
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    protected List<Row> execute(StringBuilder sb) {
        return execute(sb.toString(), TestingConstants.FETCH_SIZE).all();
    }

    protected List<Row> execute(StringBuilder sb, int fetchSize) {
        return execute(sb.toString(), fetchSize).all();
    }

    protected List<Row> execute(Statement statement) {
        return execute(statement.toString(), TestingConstants.FETCH_SIZE).all();
    }

    protected List<Row> execute(Statement statement, int fetchSize) {
        return execute(statement.toString(), fetchSize).all();
    }

    public List<Row> execute(String query) {
        return execute(query, TestingConstants.FETCH_SIZE).all();
    }

    public ResultSet executeQuery(String query) {
        return execute(query, TestingConstants.FETCH_SIZE);
    }
    protected ResultSet execute(String query, int fetchSize) {
        if (!query.endsWith(";")) query += ";";
        logger.debug("CQL: " + query);
        Statement statement = new SimpleStatement(query);
        statement.setFetchSize(fetchSize);
        return session.execute(statement);
    }


    public Session getSession() {
        return session;
    }

    public void disconnect() {
        session.close();
    }

    public CassandraUtils waitForIndexing() {

        // Waiting for the custom index to be refreshed
        logger.debug("Waiting for the index to be created...");
        try {
            Thread.sleep(TestingConstants.INDEX_WAIT_MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("Interruption caught during a Thread.sleep; index might be unstable");
        }
        logger.debug("Index ready to rock!");

        return this;
    }

    public CassandraUtils refreshIndex() {
//        session.execute(QueryBuilder.select()
//                                    .from(keyspace, table)
//                                    .where(eq(indexColumn, search().refresh(true).toJson()))
//                                    .setConsistencyLevel(ConsistencyLevel.ALL));
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
                                   .append(") USING 'com.stratio.cassandra.lucene.Index' WITH OPTIONS = {")
                                   .append("'refresh_seconds':'" + TestingConstants.INDEX_REFRESH_SECONDS + "',")
                                   .append("'ram_buffer_mb':'64',")
                                   .append("'max_merge_mb':'5',")
                                   .append("'max_cached_mb':'30',")
                                   .append(" 'schema':'{default_analyzer:")
                                   .append("\"org.apache.lucene.analysis.standard.StandardAnalyzer\",fields:{")
                                   .append(conversionCassToLucene())
                                   .append("}}'};"));

        return this;
    }

    public CassandraUtils createIndex(String indexName, SchemaBuilder schemaBuilder) {
        String schema = schemaBuilder.toJson();
        StringBuilder stBuilder = new StringBuilder().append("CREATE CUSTOM INDEX ")
                                                     .append(indexName)
                                                     .append(" ON ")
                                                     .append(qualifiedTable)
                                                     .append(" (")
                                                     .append(indexColumn)
                                                     .append(") USING 'com.stratio.cassandra.lucene.Index' WITH OPTIONS = {")
                                                     .append("'refresh_seconds':'" +
                                                             TestingConstants.INDEX_REFRESH_SECONDS +
                                                             "',")
                                                     .append("'ram_buffer_mb':'64',")
                                                     .append("'max_merge_mb':'5',")
                                                     .append("'max_cached_mb':'30',")
                                                     .append(" 'schema':'" + schema + "'")
                                                     .append("};");
        execute(stBuilder);
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
                lucenetype = lucenetype.replace("}", ", sorted:\"true\"}");
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
                lucenetype = lucenetype.replace("}", ", sorted:\"false\"}");
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
        String search = search().query(all()).refresh(true).toJson();
        return execute(QueryBuilder.select()
                                   .from(keyspace, table)
                                   .where(QueryBuilder.eq(indexColumn, search))
                                   .and(QueryBuilder.eq(name, value))
                                   .limit(limit)
                                   .allowFiltering()
                                   .setConsistencyLevel(consistencyLevel));
    }

    @SafeVarargs
    public final CassandraUtils insert(Map<String, String>... paramss) {

        Batch batch = QueryBuilder.unloggedBatch();
        for (Map<String, String> params : paramss) {
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

            batch.add(new SimpleStatement(new StringBuilder().append("INSERT INTO ")
                                                             .append(qualifiedTable)
                                                             .append(" (")
                                                             .append(columns)
                                                             .append(") VALUES (")
                                                             .append(values)
                                                             .append(");")
                                                             .toString()));
        }
        execute(batch);
        return this;
    }

    public CassandraUtils insert(String[] names, Object[] values) {
        execute(QueryBuilder.insertInto(keyspace, table).values(names, values));
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
        return new CassandraUtilsSelect(this).search().filter(all());
    }

    public CassandraUtilsSelect query(ConditionBuilder<?, ?> query) {
        return new CassandraUtilsSelect(this).query(query);
//        return new CassandraUtilsSelect(this).query(query).fetchSize(-1);
    }

    public CassandraUtilsSelect filter(ConditionBuilder<?, ?> filter) {
        return new CassandraUtilsSelect(this).filter(filter);
    }

    public CassandraUtilsSelect sort(SortFieldBuilder... sort) {
        return new CassandraUtilsSelect(this).sort(sort);
//        return new CassandraUtilsSelect(this).sort(sort).fetchSize(-1);
    }

}
