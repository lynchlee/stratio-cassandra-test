package com.stratio.cassandra.lucene.validation;

import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.datastax.driver.core.ResultSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class UDTValidationTest {

    private static CassandraUtils cassandraUtils;

    @BeforeClass
    public static void before() {

        cassandraUtils = CassandraUtils.builder()
                                       .withTable(TestingConstants.TABLE_NAME_CONSTANT)
                                       .build();
        cassandraUtils.createKeyspace();

        String useKeyspaceQuery=" USE "+cassandraUtils.getKeyspace()+" ;";
        String geoPointUdfCreateQuery="CREATE TYPE geo_point ( latitude float, longitude float );";
        String adressUdfCreateQuery="CREATE TYPE address ( street text, city text, zip int, bool boolean, hight float, point frozen<geo_point>);";
        String tableCreationQuery="CREATE TABLE "+cassandraUtils.getTable()+" ( login text PRIMARY KEY, first_name text, last_name text, addres frozen<address>, lucene text);";


        cassandraUtils.execute(useKeyspaceQuery);
        cassandraUtils.execute(geoPointUdfCreateQuery);
        cassandraUtils.execute(adressUdfCreateQuery);
        cassandraUtils.execute(tableCreationQuery);
    }




    @AfterClass
    public static void after() {
        cassandraUtils.dropTable().dropKeyspace().disconnect();
    }

    @Test
    public void testValidCreateIndex() {
        String createIndexQuery="CREATE CUSTOM INDEX "+TestingConstants.INDEX_NAME_CONSTANT+" ON "+cassandraUtils.getKeyspace()+"."+cassandraUtils.getTable()+"(lucene) "+
                                    "USING 'com.stratio.cassandra.lucene.Index' "+
                                    "WITH OPTIONS = { "+
                                    "'refresh_seconds' : '1', "+
                                    "'schema' : '{ "+
                                        " fields : { "+
                                            "\"addres.city\" : {type:\"string\"},"+
                                            "\"addres.zip\" : {type:\"integer\"},"+
                                            "\"addres.bool\" : {type:\"boolean\"}, "+
                                            "\"addres.hight\" : {type:\"float\"},"+
                                            " first_name : {type:\"string\"}}}'};";


        ResultSet result=cassandraUtils.executeQuery(createIndexQuery);
        assertEquals("Creating valid udt index must return that was applied", true, result.wasApplied());

        String dropIndex="DROP INDEX "+TestingConstants.INDEX_NAME_CONSTANT+";";
        result=cassandraUtils.executeQuery(dropIndex);
        assertEquals("Dropping valid udt index must return that was applied", true, result.wasApplied());
    }

    @Test
    public void testInValidCreateIndex() {
        String createIndexQuery="CREATE CUSTOM INDEX "+TestingConstants.INDEX_NAME_CONSTANT+" ON "+cassandraUtils.getKeyspace()+"."+cassandraUtils.getTable()+"(lucene) "+
                                "USING 'com.stratio.cassandra.lucene.Index' "+
                                "WITH OPTIONS = { "+
                                "'refresh_seconds' : '1', "+
                                "'schema' : '{ "+
                                    " fields : { "+
                                        " \"addres.inexistent.latitude\" : {type:\"string\"}}}'};";


        try {
            cassandraUtils.executeQuery(createIndexQuery);
            assertFalse("Creating invalid index must throw an Exception but does not ", true);
        } catch (InvalidConfigurationInQueryException e) {
            String expectedMessage="'schema' is invalid : No column definition 'addres.inexistent' for mapper 'addres.inexistent.latitude'";
            assertEquals("Cretaing invalid index must return InvalidConfigurationInQueryException("+expectedMessage+") but returns InvalidConfigurationInQueryException("+e.getMessage()+")",expectedMessage,e.getMessage());

        }
    }

    @Test
    public void testInValidCreateIndex2() {
        String createIndexQuery="CREATE CUSTOM INDEX "+TestingConstants.INDEX_NAME_CONSTANT+" ON "+cassandraUtils.getKeyspace()+"."+cassandraUtils.getTable()+"(lucene) "+
                                "USING 'com.stratio.cassandra.lucene.Index' "+
                                "WITH OPTIONS = { "+
                                "'refresh_seconds' : '1', "+
                                "'schema' : '{ "+
                                " fields : { "+
                                "\"addres.inexistent\" : {type:\"string\"}}}'};";

        try {
            cassandraUtils.executeQuery(createIndexQuery);
            assertFalse("Creating invalid index must throw an Exception but does not ",true);
        } catch (InvalidConfigurationInQueryException e) {
            String expectedMessage="'schema' is invalid : No column definition 'addres.inexistent' for mapper 'addres.inexistent'";
            assertEquals("Cretaing invalid index must return InvalidConfigurationInQueryException("+expectedMessage+") but returns InvalidConfigurationInQueryException("+e.getMessage()+")",expectedMessage,e.getMessage());

        }
    }

    @Test
    public void testInValidCreateIndex3() {
        String createIndexQuery="CREATE CUSTOM INDEX "+TestingConstants.INDEX_NAME_CONSTANT+" ON "+cassandraUtils.getKeyspace()+"."+cassandraUtils.getTable()+"(lucene) "+
                                "USING 'com.stratio.cassandra.lucene.Index' "+
                                "WITH OPTIONS = { "+
                                "'refresh_seconds' : '1', "+
                                "'schema' : '{ "+
                                " fields : { "+
                                    "\"addres.city\" : {type:\"string\"},"+
                                    "\"addres.zip\" : {type:\"integer\"},"+
                                    "\"addres.bool\" : {type:\"boolean\"},"+
                                    "\"addres.hight\" : {type:\"float\"},"+
                                    "\"addres.point.latitude\" : {type:\"float\"},"+
                                    "\"addres.point.longitude\" : {type:\"bytes\"},"+
                                    "first_name : {type:\"string\"}}}'};";

        try {
            cassandraUtils.executeQuery(createIndexQuery);
            assertFalse("Creating invalid index must throw an Exception but does not ",true);
        } catch (InvalidConfigurationInQueryException e) {
            String expectedMessage="'schema' is invalid : 'org.apache.cassandra.db.marshal.FloatType' is not supported by mapper 'addres.point.longitude'";
            assertEquals("Cretaing invalid index must return InvalidConfigurationInQueryException("+expectedMessage+") but returns InvalidConfigurationInQueryException("+e.getMessage()+")",expectedMessage,e.getMessage());

        }
    }

    @Test
    public void testInValidCreateIndex4() {
        String createIndexQuery="CREATE CUSTOM INDEX "+TestingConstants.INDEX_NAME_CONSTANT+" ON "+cassandraUtils.getKeyspace()+"."+cassandraUtils.getTable()+"(lucene) "+
                                "USING 'com.stratio.cassandra.lucene.Index' "+
                                "WITH OPTIONS = { "+
                                "'refresh_seconds' : '1', "+
                                "'schema' : '{ "+
                                " fields : { "+
                                    "\"addres.city\" : {type:\"string\"},"+
                                    "\"addres.zip\" : {type:\"integer\"},"+
                                    "\"addres.bool\" : {type:\"boolean\"},"+
                                    "\"addres.hight\" : {type:\"float\"},"+
                                    "\"addres.point.latitude\" : {type:\"float\"},"+
                                    "\"addres.point.longitude.inexistent\" : {type:\"float\"},"+
                                    "first_name : {type:\"string\"}}}'};";

        try {
            cassandraUtils.executeQuery(createIndexQuery);
            assertFalse("Creating invalid index must throw an Exception but does not ",true);
        } catch (InvalidConfigurationInQueryException e) {
            String expectedMessage="'schema' is invalid : No column definition 'addres.point.longitude.inexistent' for mapper 'addres.point.longitude.inexistent'";
            assertEquals("Cretaing invalid index must return InvalidConfigurationInQueryException("+expectedMessage+") but returns InvalidConfigurationInQueryException("+e.getMessage()+")",expectedMessage,e.getMessage());

        }
    }

}
