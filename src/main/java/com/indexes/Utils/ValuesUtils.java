package com.indexes.Utils;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;

import com.indexes.TestCases.TestsConsults.TestsWildcards;

/**
 * Created by Jcalderin on 26/03/14.
 */
public class ValuesUtils {
    Hashtable<String, List<String>> values = new Hashtable<String, List<String>>();
    Random rand = new Random();
    Long date = System.currentTimeMillis();
    private boolean debug = true;

    public ValuesUtils()
    {
        genExampleAllTypeOfValues();
    }
    public void genExampleAllTypeOfValues()
    {
        String[] ascii = {"'frasetipoascii'","'frase tipo ascii'"};
        String[] bigint = {"1000000000000000","2000000000000000","10"};
        String[] bool = {"true","false"};
        String[] blob = {"0x3E0A16"};
        String[] counter = {"1","1.0","1.1","0","-1"};
        String[] decimal = {"1000000000.0","1000004219.0","4185236179.0"};
        String[] double_ = {"1","1.0","1.1","0","-1"};
        String[] float_ = {"1","1.0","1.1","0","-1"};
        String[] inet = {"'8.8.8.8'","'127.0.0.1'","'192.168.1.1'"};
        String[] int_ = {"-1","0","1"};
        String[] text = {"'Frase con espacios articulos y los verbos suficientes'",
                "'Fasesinespacios'",
        "'Frase con algunos espacios y articulos colocados de forma diferente'"};
        String[] timestamp = {date.toString()};
        String[] uuid = {"60297440-b4fa-11e3-8b5a-0002a5d5c51b","6d773740-b4fa-11e3-bf12-0002a5d5c51b"};
        String[] varchar = {"'frasesencillasinespacios'", "'frase sencilla con espacios'"};
        String[] timeuuid = {"a4a70900-24e1-11df-8924-001ff3591711"};
        String[] list = {"['l1','l2']"};
        String[] set = {"{'s1','s2'}"};
        String[] map = {"{'k1':'v1','k2':'v2'"};
        values.put("ascii",Arrays.asList(ascii));
        values.put("bigint",Arrays.asList(bigint));
        values.put("boolean",Arrays.asList(bool));
        values.put("blob",Arrays.asList(blob));
        values.put("counter",Arrays.asList(counter));
        values.put("decimal",Arrays.asList(decimal));
        values.put("double",Arrays.asList(double_));
        values.put("float",Arrays.asList(float_));
        values.put("inet",Arrays.asList(inet));
        values.put("int",Arrays.asList(int_));
        values.put("text",Arrays.asList(text));
        values.put("timestamp",Arrays.asList(timestamp));
        values.put("uuid",Arrays.asList(uuid));
        values.put("varchar",Arrays.asList(varchar));
        values.put("timeuuid",Arrays.asList(timeuuid));
        values.put("list",Arrays.asList(list));
        values.put("set",Arrays.asList(set));
        values.put("map",Arrays.asList(map));
    }
    public void genEmptyAllTypeOfValues()
    {
        String[] ascii = {""};
        String[] bigint = {""};
        String[] bool = {""};
        String[] blob = {""};
        String[] counter = {""};
        String[] decimal = {""};
        String[] double_ = {""};
        String[] float_ = {""};
        String[] inet = {""};
        String[] int_ = {""};
        String[] text = {""};
        String[] timestamp = {""};
        String[] uuid = {""};
        String[] varchar = {""};
        String[] timeuuid = {""};
        String[] list = {""};
        String[] set = {""};
        String[] map = {""};
        values.put("ascii",Arrays.asList(ascii));
        values.put("bigint",Arrays.asList(bigint));
        values.put("boolean",Arrays.asList(bool));
        values.put("blob",Arrays.asList(blob));
        values.put("counter",Arrays.asList(counter));
        values.put("decimal",Arrays.asList(decimal));
        values.put("double",Arrays.asList(double_));
        values.put("float",Arrays.asList(float_));
        values.put("inet",Arrays.asList(inet));
        values.put("int",Arrays.asList(int_));
        values.put("text",Arrays.asList(text));
        values.put("timestamp",Arrays.asList(timestamp));
        values.put("uuid",Arrays.asList(uuid));
        values.put("varchar",Arrays.asList(varchar));
        values.put("timeuuid",Arrays.asList(timeuuid));
        values.put("list",Arrays.asList(list));
        values.put("set",Arrays.asList(set));
        values.put("map",Arrays.asList(map));
    }

    public void genOneSimpleAllTypeOfValues()
    {
        String[] ascii = {"ascii"};
        String[] bigint = {"10000000000"};
        String[] bool = {"true"};
        String[] blob = {"0x000000"};
        String[] counter = {"1.0"};
        String[] decimal = {"1.0"};
        String[] double_ = {"1.0"};
        String[] float_ = {"1.0"};
        String[] inet = {"'1.1.1.1'"};
        String[] int_ = {"1"};
        String[] text = {"text"};
        String[] timestamp = {"0000000000000"};//1970-01-01 01:00:00+0100
        String[] uuid = {"550e8400-e29b-41d4-a716-446655440000"};
        String[] varchar = {"varchar"};
        String[] timeuuid = {"550e8400-e29b-41d4-a716-446655440000"};
        String[] list = {"['l1','l2']"};
        String[] set = {"{'s1','s2'}"};
        String[] map = {"{'k1':'v1','k2':'v2'"};
        values.put("ascii",Arrays.asList(ascii));
        values.put("bigint",Arrays.asList(bigint));
        values.put("boolean",Arrays.asList(bool));
        values.put("blob",Arrays.asList(blob));
        values.put("counter",Arrays.asList(counter));
        values.put("decimal",Arrays.asList(decimal));
        values.put("double",Arrays.asList(double_));
        values.put("float",Arrays.asList(float_));
        values.put("inet",Arrays.asList(inet));
        values.put("int",Arrays.asList(int_));
        values.put("text",Arrays.asList(text));
        values.put("timestamp",Arrays.asList(timestamp));
        values.put("uuid",Arrays.asList(uuid));
        values.put("varchar",Arrays.asList(varchar));
        values.put("timeuuid",Arrays.asList(timeuuid));
        values.put("list",Arrays.asList(list));
        values.put("set",Arrays.asList(set));
        values.put("map",Arrays.asList(map));
    }

    public String getRandValueOfType(String type)
    {
        debug("Generando valor aleatorio del tipo dado");
        int maxIndex = values.get(type).size();
        int index = rand.nextInt(maxIndex);
        String returnValue = values.get(type).get(index);
        debug("El valor generado es: \n" + returnValue);
        return returnValue;
    }

    public Hashtable<String,String> getRandValuesOfTypesGiven(Hashtable<String,String> columnsType)
    {
        debug("Generando valores de para cada una de las columnas dadas");
        Hashtable<String,String> columnValue = new Hashtable<String, String>();
        for (String s:columnsType.keySet())
        {
            columnValue.put(s,getRandValueOfType(columnsType.get(s)));
        }
        debug("Los valores generados son: " + columnValue.toString());
        return columnValue;
    }

    private void debug (Object o)
    {
        if (debug)
        {
            TestsWildcards.mu.debug("ValuesUtils:" + o);
        }
    }

}
