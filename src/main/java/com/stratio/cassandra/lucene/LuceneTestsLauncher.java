package com.stratio.cassandra.lucene;

import org.apache.log4j.Logger;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import com.stratio.cassandra.lucene.suite.BreakDownSuite;
import com.stratio.cassandra.lucene.suite.CollectionsSuite;
import com.stratio.cassandra.lucene.suite.ComposedNumericPrimaryKeySuite;
import com.stratio.cassandra.lucene.suite.DeletionSuite;
import com.stratio.cassandra.lucene.suite.MultipleNumericPrimaryKeySuite;
import com.stratio.cassandra.lucene.suite.SingleNumericPrimaryKeySuite;
import com.stratio.cassandra.lucene.suite.SingleStringPrimaryKeySuite;
import com.stratio.cassandra.lucene.suite.SingleTextPrimaryKeySuite;
import com.stratio.cassandra.lucene.suite.StoriesSuite;
import com.stratio.cassandra.lucene.suite.VariaSuite;

public class LuceneTestsLauncher {

    private static final Logger logger = Logger
            .getLogger(LuceneTestsLauncher.class);

    public static void main(String[] args) {

        if (args.length != 2) {
            System.err
                    .println("You must set the arguments: %1 - replication factor; %2 - consistency level");
            return;
        } else {
            System.setProperty(
                    TestingConstants.REPLICATION_FACTOR_CONSTANT_NAME, args[0]);
            System.setProperty(
                    TestingConstants.CONSISTENCY_LEVEL_CONSTANT_NAME, args[1]);
        }

        Result result = JUnitCore.runClasses(BreakDownSuite.class,
                CollectionsSuite.class, ComposedNumericPrimaryKeySuite.class,
                DeletionSuite.class, MultipleNumericPrimaryKeySuite.class,
                SingleNumericPrimaryKeySuite.class,
                SingleStringPrimaryKeySuite.class,
                SingleTextPrimaryKeySuite.class, StoriesSuite.class,
                VariaSuite.class);

        logger.info("Tests finished!");
    }
}
