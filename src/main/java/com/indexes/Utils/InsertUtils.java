package com.indexes.Utils;

import com.indexes.TestCases.TestsConsults.TestsWildcards;

import java.util.Hashtable;

/**
 * Created by Jcalderin on 26/03/14.
 */
public class InsertUtils {
    private boolean debug = true;
    public String keyspace;
    public String table;
    public String indexColumn;
    public Hashtable<String,String> values;

    public InsertUtils(String keyspace, String table, String indexColumn) {
        this.keyspace = keyspace;
        this.table = table;
        this.indexColumn = indexColumn;
    }

    public void InsertValues(Hashtable<String,String> values)
    {
        debug("Insertando valores de la tabla a la tabla interna: " + values.toString());
        this.values = values;
    }
    
    public String getInsert()
    {
        String column = "";
        String value = "";
        for (String s: values.keySet())
        {
            if (s != indexColumn)
            {
                column = column + s + ",";
                value = value + values.get(s) + ",";
            }
        }
        column = column.substring(0,column.length()-1);
        value = value.substring(0,value.length()-1);
        String insert = "INSERT INTO " + keyspace + "." + table + " (" + column + ") VALUES (" + value + ");";
        debug("Insert a ejecutar: " + insert);
        return insert;
    }

    private void debug(Object o)
    {
        if (debug)
        {
            TestsWildcards.mu.debug("InsertUtils" + o);
        }
    }
}