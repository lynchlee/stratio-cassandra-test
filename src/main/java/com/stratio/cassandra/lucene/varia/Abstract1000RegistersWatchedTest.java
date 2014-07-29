/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
