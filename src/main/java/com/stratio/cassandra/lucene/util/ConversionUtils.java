package com.stratio.cassandra.lucene.util;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Jcalderin on 25/03/14.
 */
public class ConversionUtils {

    private static final Map<String, List<String>> dict;

    static {
        dict = new LinkedHashMap<>();

        // CassandraTypes with Java posibilities
        String[] Boolean = { "Boolean", "String" };
        dict.put("boolean", Arrays.asList(Boolean));
        String[] Date = { "Date", "Number", "String" };
        dict.put("date", Arrays.asList(Date));
        String[] Double = { "Number", "String" };
        dict.put("double", Arrays.asList(Double));
        String[] Float = { "Number", "String" };
        dict.put("float", Arrays.asList(Float));
        String[] Integer = { "Number", "String" };
        dict.put("integer", Arrays.asList(Integer));
        String[] Long = { "Number", "String" };
        dict.put("long", Arrays.asList(Long));
        String[] Varchar = { "Object" };
        dict.put("varchar", Arrays.asList(Varchar));
        String[] Text = { "Object" };
        dict.put("text", Arrays.asList(Text));
        String[] UUID = { "UUID", "String" };
        dict.put("uuid", Arrays.asList(UUID));
        String[] Variant = { "Object" };
        dict.put("variant", Arrays.asList(Variant));
    }

    public static Map<String, List<String>> cassandraToJava() {

        return dict;

    }
}
