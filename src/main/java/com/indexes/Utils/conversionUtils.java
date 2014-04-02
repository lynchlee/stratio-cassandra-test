package com.indexes.Utils;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

/**
 * Created by Jcalderin on 25/03/14.
 */
public class conversionUtils {
    public Hashtable<String,List<String>> cassandraToJava()
    {
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
}
