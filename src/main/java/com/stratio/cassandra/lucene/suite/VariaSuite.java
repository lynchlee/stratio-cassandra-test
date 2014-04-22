package com.stratio.cassandra.lucene.suite;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.stratio.cassandra.lucene.varia.FilterTest;

@RunWith(Suite.class)
// @SuiteClasses({ AllowFilteringWith1000SimilarRowsTest.class,
// AllowFilteringWith1000MixedRowsTest.class })
@SuiteClasses({ FilterTest.class })
public class VariaSuite {

}
