package com.indexes.TestCases.TestsConsults;

/**
 * Created by Jcalderin on 24/03/14.
 */

import com.datastax.driver.core.ResultSet;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import com.indexes.Utils.*;
import com.indexes.TestIndexes;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;


@RunWith(JUnit4.class)
public class TestsWildcards {
    private static boolean debug = true;
    private static String keyspace = "indexes";
    private static String table = "testIndexes";
    private static String indexName = "indexIndexes";
    public static String indexColumn = "lucene";
    public static MainUtils mu = new MainUtils();
    private static TableUtils tu;
    private static IndexUtils iu;
    private static ValuesUtils vu;
    private static InsertUtils insu;
    private static Hashtable<String,String> columns;

    @BeforeClass public static void setUpTests()
    {
        debug("Creando las columnas de la tabla");
        columns = new Hashtable<String, String>();
        columns.put("ascii_1","ascii");
        columns.put("bigint_1","bigint");
        columns.put("blob_1","blob");
        columns.put("boolean_1","boolean");
        //columns.put("counter_1","counter"); Las columnas counter solo se pueden menter en una tabla con solo columnas counter
        columns.put("decimal_1","decimal");
        columns.put("date_1","timestamp");
        columns.put("double_1","double");
        columns.put("float_1","float");
        columns.put("integer_1","int");
        columns.put("inet_1","inet");
        columns.put("text_1","text");
        columns.put("varchar_1","varchar");
        columns.put("uuid_1","uuid");
        columns.put("timeuuid_1","timeuuid");
        columns.put("lucene","text");
        debug("Creando la clave primaria");
        Hashtable<String,List<String>> primaryKey = new Hashtable<String, List<String>>();
        String[] inarray = {"integer_1"};
        String[] outarray = {};
        List<String> in = Arrays.asList(inarray);
        List<String> out = Arrays.asList(outarray);
        primaryKey.put("in", in);
        primaryKey.put("out",out);
        debug("Conectando con cassandra");
        mu.connect(TestIndexes.cassandra);
        tu = new TableUtils(keyspace,table,columns,primaryKey);
        iu = new IndexUtils(keyspace,table,indexName,indexColumn,columns);
        vu = new ValuesUtils();
        insu = new InsertUtils(keyspace,table,indexColumn);
        debug("Creando la tabla");
        tu.createStructure();
        debug("Creando el indice");
        iu.createIndex();
        debug("Insertando datos");
        //Genero valores random a insertar y los meto en un Insert
        insu.InsertValues(vu.getRandValuesOfTypesGiven(columns));
        //Inserto los valores
        mu.executeQuery(insu.getInsert());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        debug("Preparación finalizada, comienzan las pruebas");
        mu.session.close();
    }

    @AfterClass public static void tearDownTests()
    {
        mu.connect(TestIndexes.cassandra);
        debug("Limpiando la BBDD");
        tu.dropKeyspaceString();
        debug("Cerrando la sesión con cassandra");
        mu.session.close();
    }

    @Before
    public void setUp()
    {
        mu.connect(TestIndexes.cassandra);
        TestsWildcards.mu.opsToCassandra.add("*************************************************************");
    }

    @After
    public void tearDown()
    {
        mu.session.close();
        TestsWildcards.mu.opsToCassandra.add("*************************************************************");
    }
    //*****************
    @Test() public void testJsonStringWildcardQuery()
    {
        ResultSet resultCass,resultWildcardJson;
        //Obtengo los valores mediante datastax
        resultCass = mu.executeQuery(tu.selectAllFromTable());
        resultWildcardJson = mu.executeQuery(iu.getWildcardQueryJson("text_1", "*"));
        assertEquals(mu.resultSetToString(resultCass),mu.resultSetToString(resultWildcardJson));
    }

    @Test() public void testLuceneStringWildcardQuery()
    {
        ResultSet resultCass,resultWildcardLucene;
        //Obtengo los valores mediante datastax
        resultCass = mu.executeQuery(tu.selectAllFromTable());
        resultWildcardLucene = mu.executeQuery(iu.getWildcardQueryLucene("text_1", "*"));
        assertEquals(mu.resultSetToString(resultCass), mu.resultSetToString(resultWildcardLucene));
    }
    //****************
    @Test() public void testKOJsonBigintWildcardQuery()
    {
        ResultSet resultWildcardJson;
        //Obtengo los valores mediante datastax
        try {
            resultWildcardJson = mu.executeQuery(iu.getWildcardQueryJson("bigint_1", "*"));
            fail("No se ha lanzado excepción2");
        } catch (Exception e) {
            assertTrue(true);

        }
    }

    @Test() public void testKOLuceneBigintWildcardQuery()
    {
        ResultSet resultWildcardLucene;
        try {
            resultWildcardLucene = mu.executeQuery(iu.getWildcardQueryLucene("bigint_1", "*"));
            fail("No se ha lanzado excepción");
        } catch (Exception e) {
            assertTrue(true);
        }
    }
    //****************

    @Test() public void testJsonBytesWildcardQuery()
    {
        ResultSet resultCass,resultWildcardJson;
        //Obtengo los valores mediante datastax
        resultCass = mu.executeQuery(tu.selectAllFromTable());
        resultWildcardJson = mu.executeQuery(iu.getWildcardQueryJson("blob_1", "*"));
        assertEquals(mu.resultSetToString(resultCass),mu.resultSetToString(resultWildcardJson));
    }

    @Test() public void testLuceneBytesWildcardQuery()
    {
        ResultSet resultCass,resultWildcardLucene;
        //Obtengo los valores mediante datastax
        resultCass = mu.executeQuery(tu.selectAllFromTable());
        resultWildcardLucene = mu.executeQuery(iu.getWildcardQueryLucene("blob_1", "*"));
        assertEquals(mu.resultSetToString(resultCass),mu.resultSetToString(resultWildcardLucene));
    }

    //****************

    @Test() public void testJsonBooleanWildcardQuery()
    {
        ResultSet resultCass,resultWildcardJson;
        //Obtengo los valores mediante datastax
        resultCass = mu.executeQuery(tu.selectAllFromTable());
        resultWildcardJson = mu.executeQuery(iu.getWildcardQueryJson("boolean_1", "*"));
        assertEquals(mu.resultSetToString(resultCass),mu.resultSetToString(resultWildcardJson));
    }

    @Test() public void testLuceneBooleanWildcardQuery()
    {
        ResultSet resultCass,resultWildcardLucene;
        //Obtengo los valores mediante datastax
        resultCass = mu.executeQuery(tu.selectAllFromTable());
        resultWildcardLucene = mu.executeQuery(iu.getWildcardQueryLucene("boolean_1", "*"));
        assertEquals(mu.resultSetToString(resultCass),mu.resultSetToString(resultWildcardLucene));
    }
    //****************
    @Test() public void testKOJsonBigDecimalWildcardQuery()
    {
        ResultSet resultWildcardJson;
        //Obtengo los valores mediante datastax
        try {
            resultWildcardJson = mu.executeQuery(iu.getWildcardQueryJson("decimal_1", "*"));
            fail("No se ha lanzado excepción2");
        } catch (Exception e) {
            assertTrue(true);

        }
    }

    @Test() public void testKOLuceneBigDecimalWildcardQuery()
    {
        ResultSet resultWildcardLucene;
        try {
            resultWildcardLucene = mu.executeQuery(iu.getWildcardQueryLucene("decimal_1", "*"));
            fail("No se ha lanzado excepción");
        } catch (Exception e) {
            assertTrue(true);
        }
    }
    //****************
    @Test() public void testKOJsonDoubleWildcardQuery()
    {
        ResultSet resultWildcardJson;
        //Obtengo los valores mediante datastax
        try {
            resultWildcardJson = mu.executeQuery(iu.getWildcardQueryJson("double_1", "*"));
            fail("No se ha lanzado excepción2");
        } catch (Exception e) {
            assertTrue(true);

        }
    }

    @Test() public void testKOLuceneDoubleWildcardQuery()
    {
        ResultSet resultWildcardLucene;
        try {
            resultWildcardLucene = mu.executeQuery(iu.getWildcardQueryLucene("double_1", "*"));
            fail("No se ha lanzado excepción");
        } catch (Exception e) {
            assertTrue(true);
        }
    }
    //****************

    @Test() public void testKOJsonFloatWildcardQuery()
    {
        ResultSet resultWildcardJson;
        //Obtengo los valores mediante datastax
        try {
            resultWildcardJson = mu.executeQuery(iu.getWildcardQueryJson("float_1", "*"));
            fail("No se ha lanzado excepción2");
        } catch (Exception e) {
            assertTrue(true);

        }
    }

    @Test() public void testKOLuceneFloatWildcardQuery()
    {
        ResultSet resultWildcardLucene;
        try {
            resultWildcardLucene = mu.executeQuery(iu.getWildcardQueryLucene("float_1", "*"));
            fail("No se ha lanzado excepción");
        } catch (Exception e) {
            assertTrue(true);
        }
    }
    //****************
    @Test() public void testKOJsonIntegerWildcardQuery()
    {
        ResultSet resultWildcardJson;
        //Obtengo los valores mediante datastax
        try {
            resultWildcardJson = mu.executeQuery(iu.getWildcardQueryJson("integer_1", "*"));
            fail("No se ha lanzado excepción2");
        } catch (Exception e) {
            assertTrue(true);

        }
    }

    @Test() public void testKOLuceneIntegerWildcardQuery()
    {
        ResultSet resultWildcardLucene;
        try {
            resultWildcardLucene = mu.executeQuery(iu.getWildcardQueryLucene("integer_1", "*"));
            fail("No se ha lanzado excepción");
        } catch (Exception e) {
            assertTrue(true);
        }
    }
    //****************
    @Test() public void testKOJsonDateWildcardQuery()
    {
        ResultSet resultWildcardJson;
        //Obtengo los valores mediante datastax
        try {
            resultWildcardJson = mu.executeQuery(iu.getWildcardQueryJson("date_1", "*"));
            fail("No se ha lanzado excepción2");
        } catch (Exception e) {
            assertTrue(true);

        }
    }

    @Test() public void testKOLuceneDateWildcardQuery()
    {
        ResultSet resultWildcardLucene;
        try {
            resultWildcardLucene = mu.executeQuery(iu.getWildcardQueryLucene("date_1", "*"));
            fail("No se ha lanzado excepción");
        } catch (Exception e) {
            assertTrue(true);
        }
    }
    //****************
    @Test() public void testJsonUuidWildcardQuery()
    {
        ResultSet resultCass,resultWildcardJson;
        //Obtengo los valores mediante datastax
        resultCass = mu.executeQuery(tu.selectAllFromTable());
        resultWildcardJson = mu.executeQuery(iu.getWildcardQueryJson("uuid_1", "*"));
        assertEquals(mu.resultSetToString(resultCass),mu.resultSetToString(resultWildcardJson));
    }

    @Test() public void testLuceneUuidWildcardQuery()
    {
        ResultSet resultCass,resultWildcardLucene;
        //Obtengo los valores mediante datastax
        resultCass = mu.executeQuery(tu.selectAllFromTable());
        resultWildcardLucene = mu.executeQuery(iu.getWildcardQueryLucene("uuid_1", "*"));
        assertEquals(mu.resultSetToString(resultCass),mu.resultSetToString(resultWildcardLucene));
    }
    //****************
    

    private static void debug(Object o)
    {
        if (debug)
        {
            mu.debug("TestIndexes: " + o);
        }
    }
}
