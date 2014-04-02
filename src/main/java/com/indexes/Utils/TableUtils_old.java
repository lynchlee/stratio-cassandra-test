package com.indexes.Utils;

import java.util.*;

/**
 * Created by Jcalderin on 21/03/14.
 */
public class TableUtils_old {
    public String createTable;
    public List<String> insertInTable;
    public String keys;
    public String keyspace;

    /**
     * Función que inicializa los tipos de java que pueden ser convertidos a tipos de cassandra
     * @return un hastable con el tipo de cassandra como clave, y un listado de tipos compatibles de java como valor
     */
    public Hashtable<String,List<String>> cassandraToJava()
    {
        String[] casstypes = {"Boolean","Date","Double","Float","Integer","Long","Varchar","Text","UUID","Variant"};
        //CassandraTypes with Java posibilities
        String[] Boolean = {"Boolean", "String"};
        String[] Date = {"Date","Number","String"};
        String[] Double = {"Number","String"};
        String[] Float = {"Number","String"};
        String[] Integer = {"Number","String"};
        String[] Long = {"Number","String"};
        String[] Varchar = {"Object"};
        String[] Text = {"Object"};
        String[] UUID = {"UUID","String"};
        String[] Variant = {"Object"};

        Hashtable<String,List<String>> dict = new Hashtable<java.lang.String, List<java.lang.String>>();
        dict.put("boolean", Arrays.asList(Boolean));
        dict.put("date", Arrays.asList(Date));
        dict.put("double", Arrays.asList(Double));
        dict.put("float", Arrays.asList(Float));
        dict.put("integer", Arrays.asList(Integer));
        dict.put("long", Arrays.asList(Long));
        dict.put("varchar", Arrays.asList(Varchar));
        dict.put("text", Arrays.asList(Text));
        dict.put("uuid", Arrays.asList(UUID));
        dict.put("variant", Arrays.asList(Variant));
        return dict;


    }

    /**
     * Función que da ejemplos para cada uno de los tipos de valores de Java que se pueden pasar a cassandra
     * @return un Hashtable con el tipo de java como clave, y un listado Strings con ejemplos de cada uno de los tipos
     */
    public Hashtable<String,List<String>> exampleValuesOK()
    {
        Hashtable<String,List<String>> dict = new Hashtable<String,List<String>>();
        UUID uuid = new UUID(1,1);
        String[] Boolean = {"true", "false"};
        String[] String = {"Este texto no tiene caracteres raros", "Este texto tiene caracteres raros"};
        String[] DateSlash = {"2014/10/10","2014/02/29"};
        String[] Number = {"10", "100","0","-1"};
        String[] Object = {"10", "String"};
        String[] UUID = {uuid.toString()};
        dict.put("Boolean", Arrays.asList(Boolean));
        dict.put("String", Arrays.asList(String));
        dict.put("Date", Arrays.asList(DateSlash));
        dict.put("Number", Arrays.asList(Number));
        dict.put("Object", Arrays.asList(Object));
        dict.put("UUID", Arrays.asList(UUID));

        return dict;
    }

    /**
     * Función que da ejemplos erroneos para cada uno de los tipos de valores de Java que se pueden pasar a cassandra
     * @return un Hashtable con el tipo de java como clave, y un listado Strings con ejemplos de cada uno de los tipos
     */
    public Hashtable<String,List<String>> exampleValuesKO()
    {
        Hashtable<String,List<String>> dict = new Hashtable<String,List<String>>();
        UUID uuid = new UUID(1,1);
        String[] Boolean = {"true", "false"};
        String[] String = {"Este texto no tiene caracteres raros", "Este texto tiene caracteres raros"};
        String[] DateSlash = {"2014/10/10", "2014/25/25","20/10/10","2014/10/35","2014/02/29","2014/02/30"};
        String[] Number = {"10", "100","0","-1"};
        String[] Object = {"10", "String"};
        String[] UUID = {uuid.toString(), "0"};
        dict.put("Boolean", Arrays.asList(Boolean));
        dict.put("String", Arrays.asList(String));
        dict.put("DateSlash", Arrays.asList(DateSlash));
        dict.put("Number", Arrays.asList(Number));
        dict.put("Object", Arrays.asList(Object));
        dict.put("UUID", Arrays.asList(UUID));

        return dict;
    }

    public void generateExampleDataToInsert(String keyspace, String table)
    {
        Random rand = new Random();
        List<String> insertLists = new ArrayList<String>();
        String name = "";
        String value = "";
        int countJavaTypes;
        Hashtable<String,List<String>> types = cassandraToJava();
        Hashtable<String,List<String>> values = exampleValuesOK();
        for (String s: types.keySet())
        {
            List<String> javaTypes = types.get(s);
            countJavaTypes = 0;
            for (String s2: javaTypes)
            {
                name = name + s + countJavaTypes + "_" + s2 + ",";
                countJavaTypes = countJavaTypes + 1;
                int valuesSize = values.get(s2).size();
                int randElement = rand.nextInt(valuesSize);
                try {
                    value = value + values.get(s2).get(randElement) + ",";
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        name = name.substring(0,name.length()-1);
        value = value.substring(0,value.length()-1);
        insertInTable.add("INSERT INTO " + keyspace + "." + table + "(" + name + ") VALUES (" + value + ");");
    }

    public void generateExampleDataToCreateTable()
    {

        List<String> ColumnList = new ArrayList<String>();
        String aux = "";
        int countJavaTypes;
        Hashtable<String,List<String>> types = cassandraToJava();
        for (String s: types.keySet())
        {
            List<String> javaTypes = types.get(s);
            countJavaTypes = 0;
            for (String s2: javaTypes)
            {
                aux = s + countJavaTypes + "_" + s2 + " " + s + ",";
                countJavaTypes = countJavaTypes + 1;
            }
        }
        aux = aux.substring(0,aux.length()-1);
        createTable = aux;
    }
    
    public String firstElement()
    {
        String createquery = createTable;
        return createquery.substring(0,createquery.indexOf(' '));
    }

    public Hashtable<String, String> columns(int index)
    {
        Hashtable<String, String> auxColumns =  new Hashtable<String, String>();
        String columnsText = createTable;
        List<String> columType,keyValue;
        columType = Arrays.asList(columnsText.split(","));
        for (String s: columType)
        {
            keyValue = Arrays.asList(s.split(" "));
            auxColumns.put(keyValue.get(0),keyValue.get(1));
        }
        return auxColumns;
    }

    public String createTableIndex(String keyspace, String table, String primaryKey, int index)
    {
        String createtableStart = "CREATE TABLE " + keyspace + "." + table + "( ";

        String createtableEnd = primaryKey + ";";

        return createtableStart + createTable + createtableEnd;
    }

    public void KeysUtils(List<String> keysin, Boolean in)
    {
        String keysPrev = "(";
        for (String s: keysin)
        {
            keysPrev = keysPrev + s + ",";
        }
        keysPrev = keysPrev + ")";
        //Keys in --> (( keys ))
        //Keys out --> ( key )
        if (in == true) keysPrev = "(" + keysPrev + ")";
        keys = "PRIMARY KEY " + keysPrev + ";";
    }


    public void KeysUtils(List<String> keysin, List<String> keysout)
    {
        String keysPrev = "((";
        for (String s: keysin)
        {
            keysPrev = keysPrev + s + ",";
        }
        keysPrev = keysPrev + "),";
        for (String s: keysout)
        {
            keysPrev = keysPrev + s + ",";
        }
        keysPrev = keysPrev + ")";
        keys = "PRIMARY KEY " + keysPrev + ";";
    }

}
