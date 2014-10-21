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
package com.stratio.cassandra.lucene;

import com.stratio.cassandra.lucene.suite.*;
import org.apache.log4j.Logger;
import org.junit.runner.JUnitCore;

public class LuceneTestsLauncher {

    private static final Logger logger = Logger.getLogger(LuceneTestsLauncher.class);

    public static void main(String[] args) {

        if (args.length != 2) {
            System.err.println("You must set the arguments: %1 - replication factor; %2 - consistency level");
            return;
        } else {
            System.setProperty(TestingConstants.REPLICATION_FACTOR_CONSTANT_NAME, args[0]);
            System.setProperty(TestingConstants.CONSISTENCY_LEVEL_CONSTANT_NAME, args[1]);
        }

        JUnitCore.runClasses(ComplexNumericPrimaryKeySuite.class,
                             ComposedNumericPrimaryKeySuite.class,
                             MultipleNumericPrimaryKeySuite.class,
                             SingleNumericPrimaryKeySuite.class,
                             SingleStringPrimaryKeySuite.class,
                             SingleTextPrimaryKeySuite.class);

        JUnitCore.runClasses(DeletionSuite.class);
        JUnitCore.runClasses(IndexesSuite.class);
        JUnitCore.runClasses(VariaSuite.class);
        JUnitCore.runClasses(BreakDownSuite.class);
        JUnitCore.runClasses(StoriesSuite.class);

        logger.info("Tests finished!");
    }
}
