/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.util;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

public class RandomDataHelper {

    private static final Logger logger = Logger.getLogger(QueryUtils.class);

    private static final Map<String, List<String>> values;

    private static final Random rand = new Random();

    static {
        values = new LinkedHashMap<>();

        String[] bigint = { "1000000000000000", "2000000000000000", "10" };
        values.put("bigint", Arrays.asList(bigint));

        String[] blob = { "0x3E0A16" };
        values.put("blob", Arrays.asList(blob));

        String[] decimal = { "1000000000.0", "1000004219.0", "4185236179.0" };
        values.put("decimal", Arrays.asList(decimal));

        String[] float_ = { "1", "1.0", "1.1", "0", "-1" };
        values.put("float", Arrays.asList(float_));

        String[] int_ = { "-1", "0", "1" };
        values.put("int", Arrays.asList(int_));

        String[] timestamp = { String.valueOf(System.currentTimeMillis()) };
        values.put("timestamp", Arrays.asList(timestamp));

        String[] varchar = { "'frasesencillasinespacios'",
                "'frase sencilla con espacios'" };
        values.put("varchar", Arrays.asList(varchar));

        String[] list = { "['l1','l2']" };
        values.put("list", Arrays.asList(list));

        String[] map = { "{'k1':'v1','k2':'v2'" };
        values.put("map", Arrays.asList(map));

        String[] ascii = { "'frasetipoascii'", "'frase tipo ascii'" };
        values.put("ascii", Arrays.asList(ascii));

        String[] bool = { "true", "false" };
        values.put("boolean", Arrays.asList(bool));

        String[] counter = { "1", "1.0", "1.1", "0", "-1" };
        values.put("counter", Arrays.asList(counter));

        String[] double_ = { "1", "1.0", "1.1", "0", "-1" };
        values.put("double", Arrays.asList(double_));

        String[] inet = { "'8.8.8.8'", "'127.0.0.1'", "'192.168.1.1'" };
        values.put("inet", Arrays.asList(inet));

        String[] text = {
                "'Frase con espacios articulos y los verbos suficientes'",
                "'Fasesinespacios'",
                "'Frase con algunos espacios y articulos colocados de forma diferente'" };
        values.put("text", Arrays.asList(text));

        String[] uuid = { "60297440-b4fa-11e3-8b5a-0002a5d5c51b",
                "6d773740-b4fa-11e3-bf12-0002a5d5c51b" };
        values.put("uuid", Arrays.asList(uuid));

        String[] timeuuid = { "a4a70900-24e1-11df-8924-001ff3591711" };
        values.put("timeuuid", Arrays.asList(timeuuid));

        String[] set = { "{'s1','s2'}" };
        values.put("set", Arrays.asList(set));
    }

    public static Map<String, String> getRandValuesOfTypesGiven(
            Map<String, String> columnsType) {

        logger.debug("Generando valores de para cada una de las columnas dadas");
        Map<String, String> columnValue = new LinkedHashMap<String, String>();
        for (String s : columnsType.keySet()) {
            columnValue.put(s, getRandValueOfType(columnsType.get(s)));
        }
        logger.debug("Los valores generados son: " + columnValue.toString());

        return columnValue;
    }

    public static String getRandValueOfType(String type) {

        logger.debug("Generando valor aleatorio del tipo dado");
        int maxIndex = values.get(type).size();
        int index = rand.nextInt(maxIndex);
        String returnValue = values.get(type).get(index);
        logger.debug("El valor generado es: \n" + returnValue);

        return returnValue;
    }
}
