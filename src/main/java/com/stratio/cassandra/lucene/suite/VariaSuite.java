package com.stratio.cassandra.lucene.suite;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.stratio.cassandra.lucene.varia.AllowFilteringWith1000MixedRowsTest;
import com.stratio.cassandra.lucene.varia.AllowFilteringWith1000SimilarRowsTest;

@RunWith(Suite.class)
@SuiteClasses({ AllowFilteringWith1000SimilarRowsTest.class,
        AllowFilteringWith1000MixedRowsTest.class })
public class VariaSuite {

}
