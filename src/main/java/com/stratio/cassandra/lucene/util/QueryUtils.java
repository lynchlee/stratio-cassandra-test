package com.stratio.cassandra.lucene.util;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Created by Jcalderin on 21/03/14.
 */
public class QueryUtils {

    private static final Logger logger = Logger.getLogger(QueryUtils.class);

    private static final Map<String, String> conversionCassandraLucene;

    static {
        // http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/cql_data_types_c.html
        conversionCassandraLucene = new LinkedHashMap<>();

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
    }

    private final String keyspace;

    private final String table;

    private final Map<String, String> columns;

    private final Map<String, List<String>> primaryKey;

    private final String indexColumn;

    private final String columnsWithoutLucene;

    public QueryUtils(String keyspace, String table,
            Map<String, String> columns, Map<String, List<String>> primaryKey,
            String indexColumn) {

        this.keyspace = keyspace;
        this.table = table;
        this.columns = columns;
        this.primaryKey = primaryKey;
        this.indexColumn = indexColumn;
        this.columnsWithoutLucene = columnsWithoutLucene();
    }

    public Map<String, String> getColumnsWithoutLucene() {
        columns.remove(indexColumn);
        return columns;
    }

    private String columnsWithoutLucene() {

        String result = "";
        for (String s : columns.keySet()) {
            if (s != indexColumn) {
                result = result + s + ",";
            }
        }
        result = result.substring(0, result.length() - 1);

        return result;
    }

    public String createKeyspaceQuery(int replicationFactor) {

        String query = "create keyspace "
                + keyspace
                + " with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : '"
                + (replicationFactor > 0 ? replicationFactor : 1) + "' };";
        logger.debug("Keyspace creation query [" + query + "]");

        return query;
    }

    public String dropKeyspaceQuery() {

        String query = "drop keyspace " + keyspace + " ;";
        logger.debug("Keyspace deletion query [" + query + "]");

        return query;
    }

    public String createTableQuery() {

        String columnType = "";
        for (String s : columns.keySet()) {
            columnType = columnType + s + " " + columns.get(s) + ",";
        }

        String primaryKeyString = createPrimaryKeyQuery();
        String query = "CREATE TABLE " + keyspace + "." + table + " ("
                + columnType + primaryKeyString + ");";
        logger.debug("Tabla creation [" + query + "]");

        return query;
    }

    public String truncateTableQuery() {

        String query = "truncate " + keyspace + "." + table + " ;";
        logger.debug("Tabla truncation [" + query + "]");

        return query;
    }

    private String createPrimaryKeyQuery() {

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

    public String createIndex(String indexName) {
        String indexStart = "CREATE CUSTOM INDEX "
                + indexName
                + " ON "
                + keyspace
                + "."
                + table
                + " ("
                + indexColumn
                + ") USING 'org.apache.cassandra.db.index.stratio.RowIndex' WITH OPTIONS = {"
                + "'refresh_seconds':'1',"
                + "'num_cached_filters':'1',"
                + "'ram_buffer_mb':'64',"
                + "'max_merge_mb':'5',"
                + "'max_cached_mb':'30',"
                + " 'schema':'{default_analyzer:\"org.apache.lucene.analysis.standard.StandardAnalyzer\",fields:{";
        String indexMed = conversionCassToLucene();

        String indexEnd = "}}'};";
        String query = indexStart + indexMed + indexEnd;
        logger.debug("Index creation query [" + query + "]");

        return query;
    }

    public String conversionCassToLucene() {
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
                String type = typeComp.substring(indexTypeStart + 1,
                        indexTypeEnd);
                if (type.contains(",")) {
                    type = type.substring(0, type.indexOf(","));
                }

                String lucenetype = conversionCassandraLucene.get(type);
                converted = converted + s + lucenetype + ",";
            }
        }
        return converted.substring(0, converted.length() - 1);
    }

    public String selectAllFromTable() {

        String query = "SELECT " + columnsWithoutLucene + " FROM " + keyspace
                + "." + table + ";";
        logger.debug("Selectall query [" + query + "]");

        return query;
    }

    public String getInsert(Map<String, String> values) {

        String column = "";
        String value = "";
        for (String s : values.keySet()) {
            if (s != indexColumn) {
                column = column + s + ",";
                value = value + values.get(s) + ",";
            }
        }
        column = column.substring(0, column.length() - 1);
        value = value.substring(0, value.length() - 1);

        String insert = "INSERT INTO " + keyspace + "." + table + " (" + column
                + ") VALUES (" + value + ");";
        logger.debug("Insert query [" + insert + "]");

        return insert;
    }

    public String constructDeleteQueryByCondition(String condition) {

        String query = "DELETE FROM " + keyspace + "." + table + " WHERE "
                + condition + ";";
        logger.debug("Deletion query [" + query + "]");

        return query;
    }

    public String getTypedQuery(String type, String field, String value,
            Map<String, String> params) {

        StringBuffer query = new StringBuffer();
        query.append("SELECT ").append(columnsWithoutLucene).append(" FROM ")
                .append(keyspace).append(".").append(table).append(" WHERE ")
                .append(indexColumn).append("='{query:{type:\"").append(type)
                .append("\", field:\"").append(field).append("\", value:\"")
                .append(value).append("\"");
        if (params != null) {
            for (Map.Entry<String, String> param : params.entrySet()) {
                query.append(", ").append(param.getKey()).append(":")
                        .append(param.getValue());
            }
        }
        query.append("}}';");
        logger.debug("Typed query: " + query);

        return query.toString();
    }

    public String getFuzzyQuery(String field, String value,
            Map<String, String> params) {

        return getTypedQuery("fuzzy", field, value, params);
    }

    public String getMatchQuery(String field, String value,
            Map<String, String> params) {

        return getTypedQuery("match", field, value, params);
    }

    public String getPrefixQuery(String field, String value,
            Map<String, String> params) {

        return getTypedQuery("prefix", field, value, params);
    }

    public String getWildcardQuery(String field, String value,
            Map<String, String> params) {

        return getTypedQuery("wildcard", field, value, params);
    }
}