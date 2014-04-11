package com.stratio.cassandra.lucene.util;

import java.util.List;

import com.datastax.driver.core.Row;

public class IndexHandlingUtils {

    public static boolean containsElementByIntegerKey(List<Row> resultList,
            int key) {

        boolean isContained = false;
        int elementKey;
        for (Row resultElement : resultList) {
            elementKey = resultElement.getInt("integer_1");

            if (elementKey == key)
                isContained = true;
        }

        return isContained;
    }
}
