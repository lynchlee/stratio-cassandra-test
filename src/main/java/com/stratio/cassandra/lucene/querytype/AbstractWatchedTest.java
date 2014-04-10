package com.stratio.cassandra.lucene.querytype;

import org.apache.log4j.Logger;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public abstract class AbstractWatchedTest {

    private static final Logger logger = Logger.getLogger(MatchTest.class);

    private static long startingTime;

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
