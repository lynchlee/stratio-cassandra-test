package com.stratio.cassandra.lucene.suite;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.stratio.cassandra.lucene.varia.AllowFilteringWith1000MixedRowsTest;
import com.stratio.cassandra.lucene.varia.AllowFilteringWith1000SimilarRowsTest;
import com.stratio.cassandra.lucene.varia.FilterTest;
import com.stratio.cassandra.lucene.varia.TokenCriteriaWithComplexKeyTest;

@RunWith(Suite.class)
@SuiteClasses({ AllowFilteringWith1000SimilarRowsTest.class,
        AllowFilteringWith1000MixedRowsTest.class,
        TokenCriteriaWithComplexKeyTest.class, FilterTest.class })
public class VariaSuite {

}
