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
package com.stratio.cassandra.lucene.querytype;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.search.SearchBuilders.none;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class NoneTest extends AbstractWatchedTest {

    @Test
    public void noneQueryTest() {
        int n = cassandraUtils.query(none()).count();
        assertEquals("Expected 0 result!", 0, n);
    }

    @Test
    public void noneFilterTest() {
        int n = cassandraUtils.filter(none()).count();
        assertEquals("Expected 0 result!", 0, n);
    }

    @Test
    public void noneFilteredQueryTest() {
        int n = cassandraUtils.filter(none()).query(none()).count();
        assertEquals("Expected 0 result!", 0, n);
    }
}
