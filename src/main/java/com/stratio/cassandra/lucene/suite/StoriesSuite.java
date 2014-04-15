package com.stratio.cassandra.lucene.suite;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.stratio.cassandra.lucene.story.ComplexKeyDataHandlingTest;
import com.stratio.cassandra.lucene.story.ComposedKeyDataHandlingTest;
import com.stratio.cassandra.lucene.story.MultipleKeyDataHandlingTest;
import com.stratio.cassandra.lucene.story.SimpleKeyDataHandlingTest;

@RunWith(Suite.class)
@SuiteClasses({ SimpleKeyDataHandlingTest.class,
        ComposedKeyDataHandlingTest.class, ComplexKeyDataHandlingTest.class,
        MultipleKeyDataHandlingTest.class })
public class StoriesSuite {

}
