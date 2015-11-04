package com.stratio.cassandra.lucene.suite;

import com.stratio.cassandra.lucene.udt.UDTIndexing;
import com.stratio.cassandra.lucene.udt.UDTValidationTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({UDTValidationTest.class,
                     UDTIndexing.class})
public class UDTSuite {

}
