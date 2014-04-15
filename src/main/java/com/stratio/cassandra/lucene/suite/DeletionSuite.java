package com.stratio.cassandra.lucene.suite;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.stratio.cassandra.lucene.deletion.ComplexKeyDataDeletionTest;
import com.stratio.cassandra.lucene.deletion.ComposedKeyDataDeletionTest;
import com.stratio.cassandra.lucene.deletion.MultipleKeyDataDeletionTest;
import com.stratio.cassandra.lucene.deletion.SimpleKeyDataDeletionTest;

@RunWith(Suite.class)
@SuiteClasses({ SimpleKeyDataDeletionTest.class,
        ComplexKeyDataDeletionTest.class, ComposedKeyDataDeletionTest.class,
        MultipleKeyDataDeletionTest.class })
public class DeletionSuite {

}
