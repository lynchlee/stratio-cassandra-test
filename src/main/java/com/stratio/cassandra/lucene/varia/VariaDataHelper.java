package com.stratio.cassandra.lucene.varia;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.stratio.cassandra.lucene.util.QueryUtils;

public final class VariaDataHelper {

    protected static final Map<String, String> data1;

    static {
        data1 = new LinkedHashMap<>();

        data1.put("ascii_1", "'ascii'");
        data1.put("bigint_1", "1000000000000000");
        data1.put("blob_1", "0x3E0A16");
        data1.put("boolean_1", "true");
        data1.put("decimal_1", "1000000000.0");
        data1.put("date_1", String.valueOf(System.currentTimeMillis()));
        data1.put("float_1", "1.0");
        data1.put("inet_1", "'127.0.0.1'");
        data1.put("text_1", "'text'");
        data1.put("varchar_1", "'varchar'");
        data1.put("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51b");
        data1.put("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591711");
        data1.put("list_1", "['l1','l2']");
        data1.put("set_1", "{'s1','s2'}");
        data1.put("map_1", "{'k1':'v1','k2':'v2'}");
    }

    protected static List<String> generateCustomInsertions(
            int insertionsNumber, QueryUtils queryUtils) {

        return generateCustomInsertionsWithModule(insertionsNumber, 1,
                queryUtils);
    }

    protected static List<String> generateCustomInsertionsWithModule(
            int insertionsNumber, int module, QueryUtils queryUtils) {

        List<String> queriesList = new LinkedList<>();

        for (int i = 0; i < insertionsNumber; i++) {
            Map<String, String> dataAux = new LinkedHashMap<>();

            dataAux.putAll(data1);
            dataAux.put("integer_1", String.valueOf(i));
            dataAux.put("double_1", String.valueOf((i % module) + 1));

            queriesList.add(queryUtils.getInsert(dataAux));
        }

        return queriesList;
    }
}
