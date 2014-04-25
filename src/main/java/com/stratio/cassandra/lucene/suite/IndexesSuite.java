package com.stratio.cassandra.lucene.suite;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.stratio.cassandra.lucene.indexes.ComplexKeyIndexHandlingTest;
import com.stratio.cassandra.lucene.indexes.ComposedKeyIndexHandlingTest;
import com.stratio.cassandra.lucene.indexes.MultipleKeyIndexHandlingTest;
import com.stratio.cassandra.lucene.indexes.SimpleKeyIndexHandlingTest;

@RunWith(Suite.class)
@SuiteClasses({ ComplexKeyIndexHandlingTest.class,
        ComposedKeyIndexHandlingTest.class, MultipleKeyIndexHandlingTest.class,
        SimpleKeyIndexHandlingTest.class })
public class IndexesSuite {

}
