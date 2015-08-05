package com.stratio.cassandra.lucene.varia;

import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import static com.stratio.cassandra.lucene.search.SearchBuilders.bool;
import static com.stratio.cassandra.lucene.search.SearchBuilders.match;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class LargFieldTest {

    @Test
    public void tetstLargeField() throws IOException {

        CassandraUtils cassandraUtils = CassandraUtils.builder()
                                       .withTable(TestingConstants.TABLE_NAME_CONSTANT)
                                       .withIndexColumn(TestingConstants.INDEX_COLUMN_CONSTANT)
                                       .withPartitionKey("id")
                                       .withClusteringKey("name", "age")
                                       .withColumn("id", "varchar")
                                       .withColumn("name", "varchar")
                                       .withColumn("age", "varchar")
                                       .withColumn("data", "varchar")
                                       .withColumn("lucene", "varchar")
                                       .build()
                                       .createKeyspace()
                                       .createTable()
                                       .createIndex(TestingConstants.INDEX_NAME_CONSTANT)
                                       ;

        int numNumbers = 5000;
        UUID[] numbers = new UUID[numNumbers];
        for (int i = 0; i < numNumbers; i++) {
            numbers[i] = UUID.randomUUID();
        }
        String largeString = JsonSerializer.toString(numbers);

        cassandraUtils.insert(new String[]{"id","name","age","data"},new Object[]{"2","b","2","good_dat"});
        cassandraUtils.insert(new String[]{"id","name","age","data"},new Object[]{"1","a","1",largeString});

        int n1 = cassandraUtils.query(bool().must(match("id", "2")).must(match("name", "b"))).count();
        Assert.assertEquals(1, n1);

        int n2 = cassandraUtils.query(bool().must(match("id", "1")).must(match("name", "a"))).count();
        Assert.assertEquals(1, n2);

        cassandraUtils.dropIndex(TestingConstants.INDEX_NAME_CONSTANT).dropTable().dropKeyspace();
    }
}
