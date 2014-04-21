package com.stratio.cassandra.lucene.suite;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.stratio.cassandra.lucene.story.ComplexKeyDataHandlingTest;
import com.stratio.cassandra.lucene.story.ComplexKeyIndexHandlingTest;
import com.stratio.cassandra.lucene.story.ComposedKeyDataHandlingTest;
import com.stratio.cassandra.lucene.story.ComposedKeyIndexHandlingTest;
import com.stratio.cassandra.lucene.story.MultipleKeyDataHandlingTest;
import com.stratio.cassandra.lucene.story.MultipleKeyIndexHandlingTest;
import com.stratio.cassandra.lucene.story.SimpleKeyDataHandlingTest;
import com.stratio.cassandra.lucene.story.SimpleKeyIndexHandlingTest;

@RunWith(Suite.class)
@SuiteClasses({ SimpleKeyDataHandlingTest.class,
        ComposedKeyDataHandlingTest.class, ComplexKeyDataHandlingTest.class,
        MultipleKeyDataHandlingTest.class, SimpleKeyIndexHandlingTest.class,
        ComposedKeyIndexHandlingTest.class, ComplexKeyIndexHandlingTest.class,
        MultipleKeyIndexHandlingTest.class })
public class StoriesSuite {

}
