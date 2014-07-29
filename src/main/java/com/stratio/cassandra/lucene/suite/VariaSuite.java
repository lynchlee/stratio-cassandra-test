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

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.stratio.cassandra.lucene.varia.AllowFilteringWith1000MixedRowsTest;
import com.stratio.cassandra.lucene.varia.AllowFilteringWith1000SimilarRowsTest;
import com.stratio.cassandra.lucene.varia.FilterTest;
import com.stratio.cassandra.lucene.varia.TokenCriteriaWithComplexKeyTest;

@RunWith(Suite.class)
@SuiteClasses({ AllowFilteringWith1000SimilarRowsTest.class,
        AllowFilteringWith1000MixedRowsTest.class,
        TokenCriteriaWithComplexKeyTest.class, FilterTest.class })
public class VariaSuite {

}
