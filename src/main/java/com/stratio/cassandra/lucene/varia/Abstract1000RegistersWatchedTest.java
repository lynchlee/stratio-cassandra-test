package com.stratio.cassandra.lucene.varia;

import org.apache.log4j.Logger;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.QueryUtils;

public abstract class Abstract1000RegistersWatchedTest {

    private static final Logger logger = Logger
            .getLogger(Abstract1000RegistersWatchedTest.class);

    private static long startingTime;

    protected static QueryUtils queryUtils;

    protected static CassandraUtils cassandraUtils;

    @Rule
    public TestRule watcher = new TestWatcher() {

        protected void starting(Description description) {
            logger.info("*************************** Starting test ["
                    + description.getMethodName()
                    + "] ****************************");
            startingTime = System.currentTimeMillis();
        }

        protected void finished(Description description) {
            logger.info("------------------ Duration ["
                    + (System.currentTimeMillis() - startingTime)
                    + " ms] --------------------");
            logger.info("************************************************************************************************************");
        }
    };
}
