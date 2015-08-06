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

import static com.stratio.cassandra.lucene.search.SearchBuilders.all;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class AllTest extends AbstractWatchedTest {

    @Test
    public void allQueryTest() {
        int n = cassandraUtils.query(all()).count();
        assertEquals("Expected 5 result!", 5, n);
    }

    @Test
    public void allFilterTest() {
        int n = cassandraUtils.filter(all()).count();
        assertEquals("Expected 5 result!", 5, n);
    }

    @Test
    public void allFilteredQueryTest() {
        int n = cassandraUtils.filter(all()).query(all()).count();
        assertEquals("Expected 5 result!", 5, n);
    }
}
