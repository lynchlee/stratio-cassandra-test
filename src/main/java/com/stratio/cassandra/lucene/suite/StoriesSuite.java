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
package com.stratio.cassandra.lucene.suite;

import com.stratio.cassandra.lucene.story.ComplexKeyDataHandlingTest;
import com.stratio.cassandra.lucene.story.ComplexKeyIndexHandlingTest;
import com.stratio.cassandra.lucene.story.ComposedKeyDataHandlingTest;
import com.stratio.cassandra.lucene.story.ComposedKeyIndexHandlingTest;
import com.stratio.cassandra.lucene.story.MultipleKeyDataHandlingTest;
import com.stratio.cassandra.lucene.story.MultipleKeyIndexHandlingTest;
import com.stratio.cassandra.lucene.story.SimpleKeyDataHandlingTest;
import com.stratio.cassandra.lucene.story.SimpleKeyIndexHandlingTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({SimpleKeyDataHandlingTest.class,
               ComposedKeyDataHandlingTest.class,
               ComplexKeyDataHandlingTest.class,
               MultipleKeyDataHandlingTest.class,
               SimpleKeyIndexHandlingTest.class,
               ComposedKeyIndexHandlingTest.class,
               ComplexKeyIndexHandlingTest.class,
               MultipleKeyIndexHandlingTest.class})
public class StoriesSuite {

}
