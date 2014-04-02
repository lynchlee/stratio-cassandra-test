package com.indexes.Utils;


import com.indexes.TestCases.TestsConsults.TestsWildcards;

import java.util.Hashtable;

/**
 * Created by Jcalderin on 24/03/14.
 */
public class IndexUtils {
    private boolean debug = true;
    private String keyspace;
    private String table;
    private String indexName;
    private static String indexColumn;
    private Hashtable<String, String> columnsType;

    public IndexUtils(String keyspace, String table, String indexName, String indexColumn, Hashtable<String, String> columnsType) {
        this.keyspace = keyspace;
        this.table = table;
        this.indexName = indexName;
        this.indexColumn = indexColumn;
        this.columnsType = columnsType;
    }

    public void createIndex()
    {
        String indexStart = "CREATE CUSTOM INDEX " + indexName + " ON " + keyspace + "." + table + " (" + indexColumn
                + ") USING 'org.apache.cassandra.db.index.stratio.RowIndex' WITH OPTIONS = {" +
                "'refresh_seconds':'1'," +
                "'num_cached_filters':'1'," +
                "'ram_buffer_mb':'64'," +
                "'max_merge_mb':'5'," +
                "'max_cached_mb':'30'," +
                " 'schema':'{default_analyzer:\"org.apache.lucene.analysis.standard.StandardAnalyzer\",fields:{";
        String indexMed = conversionCassToLucene();

        String indexEnd = "}}'};";
        String query = indexStart + indexMed + indexEnd;
        debug("Creando el indice: " + query);
        TestsWildcards.mu.executeQuery(query);

    }

    public String conversionCassToLucene()
    {
        //http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/cql_data_types_c.html
        Hashtable<String,String> conversionCassandraLucene = new Hashtable<String, String>();
        conversionCassandraLucene.put("ascii",":{type:\"string\"}");
        conversionCassandraLucene.put("bigint",":{type:\"long\"}");
        conversionCassandraLucene.put("blob",":{type:\"bytes\"}");
        conversionCassandraLucene.put("boolean",":{type:\"boolean\"}");
        conversionCassandraLucene.put("counter",":{type:\"long\"}");
        conversionCassandraLucene.put("decimal",":{type:\"bigdec\", integer_digits:10, decimal_digits:10}");
        conversionCassandraLucene.put("double",":{type:\"double\"}");
        conversionCassandraLucene.put("float",":{type:\"float\"}");
        conversionCassandraLucene.put("inet",":{type:\"string\"}");
        conversionCassandraLucene.put("int",":{type:\"integer\"}");
        conversionCassandraLucene.put("text",":{type:\"string\"}");
        conversionCassandraLucene.put("timestamp",":{type:\"date\", pattern: \"yyyy/MM/dd\"}");
        conversionCassandraLucene.put("uuid",":{type:\"uuid\"}");
        conversionCassandraLucene.put("timeuuid",":{type:\"uuid\"}");
        conversionCassandraLucene.put("varchar",":{type:\"string\"}");
        conversionCassandraLucene.put("varint",":{type:\"bigint\", digits:10}");
        String converted = "";
        for (String s: columnsType.keySet())
        {
            if (!columnsType.get(s).endsWith(">"))
            {
                String type = columnsType.get(s);
                String lucenetype = conversionCassandraLucene.get(type);
                converted = converted + s + lucenetype + ",";
            }
            else
            {
                String typeComp = columnsType.get(s);
                int indexTypeStart = typeComp.indexOf("<");
                int indexTypeEnd = typeComp.indexOf(">");
                String type = typeComp.substring(indexTypeStart,indexTypeEnd);
                String lucenetype = conversionCassandraLucene.get(type);
                converted = converted + s + lucenetype + ",";
            }
        }
        return converted.substring(0,converted.length()-1);
    }

    public String getMatchQueryLucene(String field,String value)
    {
        String query = "SELECT " + TestsWildcards.mu.columnsWithoutLucene(columnsType,indexColumn) + " FROM " + keyspace  + "." + table + " WHERE " + indexColumn + "='" + field + ":" + value + "';";
        debug("getMatchQueryLucene: " + query);
        return query;
    }

    public String getMatchQueryJson(String field,String value)
    {
        String query = "SELECT " + TestsWildcards.mu.columnsWithoutLucene(columnsType,indexColumn) + " FROM " + keyspace  + "." + table + " WHERE " + indexColumn + "='{query:{type:\"match\", field:\"" + field + "\", value:\"" + value + "\"}}';";
        debug("getMatchQueryJson: " + query);
        return query;
    }

    public String getWildcardQueryLucene(String field,String value)
    {
        String query = "SELECT " + TestsWildcards.mu.columnsWithoutLucene(columnsType,indexColumn) + " FROM " + keyspace  + "." + table + " WHERE " + indexColumn + "='" + field + ":" + value + "';";
        debug("getWildcardQueryLucene: " + query);
        return query;
    }

    public String getWildcardQueryJson(String field,String value)
    {
        String query = "SELECT " + TestsWildcards.mu.columnsWithoutLucene(columnsType,indexColumn) + " FROM " + keyspace  + "." + table + " WHERE " + indexColumn + "='{query:{type:\"wildcard\", field:\"" + field + "\", value:\"" + value + "\"}}';";
        debug("getWildcardQueryJson: " + query);
        return query;
    }

    private void debug(Object o)

    {
        if (debug)
        {
            TestsWildcards.mu.debug("IndexUtils: " + o);
        }
    }

}
