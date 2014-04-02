package com.indexes.Utils;

/**
 * Created by Jcalderin on 3/03/14.
 */

import com.datastax.driver.core.*;
import com.indexes.TestIndexes;
import java.io.Serializable;
import java.util.*;

public class MainUtils implements Serializable{
    public Cluster cluster;
    public Session session;
    public Metadata metadata;
    public static List<String>opsToCassandra = new ArrayList<String>();


    public void connect(String node)
    {
        cluster = Cluster.builder().addContactPoint(node).build();
        metadata = cluster.getMetadata();
        debug("Connected to cluster (" + node + "): " + metadata.getClusterName() + "\n");
        session = cluster.connect();
    }

    public ResultSet executeQuery(String query)
    {
        debug("Ejecutando query: " + query);
        opsToCassandra.add(query);
        ResultSet result = null;
        result = session.execute(query);
        return result;
    }

//    public void close()
//    {
//        cluster.shutdown();
//    }

//    public ResultSet queryCassandra( String query)
//    {
//        debug("Ejecutando query a cassandra: " + query);
//        return session.execute(query);
//    }
//    //*********************************************************
//    public void createTestsKeyspace(String keyspace)
//    {
//        String queryCreateKeyspace = "create keyspace " + keyspace + " with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };";
//        debug("Creando keyspace: " + queryCreateKeyspace);
//        session.execute(queryCreateKeyspace);
//    }
//
//    public void cleanTestsKeyspace(String keyspace)
//    {
//        String dropTableQuery = "drop keyspace " + keyspace + ";";
//        debug("Limpiando Keyspace: " + dropTableQuery);
//        session.execute(dropTableQuery);
//    }

    //*****
    public String resultSetToString(ResultSet result)
    {
        String resultset = "";
        for (Row r : result.all())
        {
            resultset = resultset + r.toString();
        }
        debug("Result to String: " + resultset + "\n");
        return resultset;
    }

    public void debug(Object o)
    {
        if (TestIndexes.debug)
        {
            System.out.println(o);
        }
    }

    public String columnsWithoutLucene(Hashtable<String,String> columnsType, String indexColumn)
    {
        String result = "";
        for (String s : columnsType.keySet())
        {
            if (s != indexColumn)
            {
                result = result + s + ",";
            }
        }
        result = result.substring(0,result.length()-1);
        return result;
    }

}



