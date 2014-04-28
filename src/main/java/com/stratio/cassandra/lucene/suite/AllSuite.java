package com.stratio.cassandra.lucene.suite;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ CollectionsSuite.class, ComplexNumericPrimaryKeySuite.class,
        ComposedNumericPrimaryKeySuite.class, DeletionSuite.class,
        IndexesSuite.class, MultipleNumericPrimaryKeySuite.class,
        SingleNumericPrimaryKeySuite.class, SingleStringPrimaryKeySuite.class,
        SingleTextPrimaryKeySuite.class, VariaSuite.class,
        BreakDownSuite.class, StoriesSuite.class })
public class AllSuite {

}
