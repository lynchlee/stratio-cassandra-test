package com.indexes.Utils;

import java.util.*;

import com.indexes.TestCases.TestsConsults.TestsWildcards;


/**
 * Created by Jcalderin on 21/03/14.
 */
public class TableUtils {
    private boolean debug = true;
    public String keyspace;
    public String table;
    public Hashtable<String,String> columns;
    public Hashtable<String,List<String>> primaryKey;

    public TableUtils(String keyspace,String table,Hashtable<String,String> columns, Hashtable<String,List<String>> primaryKey)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.columns = columns;
        this.primaryKey = primaryKey;
    }

    public void createStructure()
    {
        debug("Creando estructura: KeySpace y Tabla");
        createKeyspaceString();
        createTable();
    }

    public void createKeyspaceString()
    {
        //MakeQuery
        String query = "create keyspace " + keyspace + " with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };";
        //Execute in session
        debug("Creando keyspace: " + query);
        TestsWildcards.mu.executeQuery(query);
    }

    public void dropKeyspaceString()
    {
        //MakeQuery
        String query = "drop keyspace " + keyspace + " ;";
        //Execute in session
        debug("Limpiando keyspace: " + query);
        TestsWildcards.mu.executeQuery(query);
    }

    public void createKeyspaceStatement()
    {
        //Other use of datastax
    }

    public void createTable()
    {
        String columnType = "";
        //Convert HashTable of columns to string
        for (String s: columns.keySet())
        {
            columnType = columnType + s + " " + columns.get(s) + ",";
        }
        //Create de primary key
        String primaryKeyString = createPrimaryKey();
        String query = "CREATE TABLE " + keyspace + "." + table + " (" + columnType + primaryKeyString + ");";
        debug("Creando tabla: "+ query);
        TestsWildcards.mu.executeQuery(query);
    }

    private String createPrimaryKey()
    {
        String in = "";
        String out = "";
        if (primaryKey.isEmpty()) return "";
        if (primaryKey.containsKey("in"))
        {
            for (String s: primaryKey.get("in"))
            {
                in = in + s + ",";
            }
        }
        if (primaryKey.containsKey("out"))
        {
            for (String s: primaryKey.get("out"))
            {
                out = out + s + ",";
            }
        }

        String res = "PRIMARY KEY ";
        if (out != ""){
            out = out.substring(0,out.length()-1);
            out = "," + out + ")";
            res = res + "(";
        }
        else
        if (in != ""){
            in = in.substring(0,in.length()-1);
            in = "(" + in + ")";
        }
        String result = res + in + out;
        return result;


    }

    public String selectAllFromTable()
    {
        String query = "SELECT " + TestsWildcards.mu.columnsWithoutLucene(columns, TestsWildcards.indexColumn) + " FROM " + keyspace + "." + table + ";";
        debug("Selectall: " + query);
        return query;
    }

    private void debug(Object o)
    {
        if (debug)
        {
            TestsWildcards.mu.debug("TableUtils: " + o);
        }
    }

}