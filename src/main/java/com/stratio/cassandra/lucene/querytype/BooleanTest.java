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

import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.TestingConstants;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static com.stratio.cassandra.lucene.search.SearchBuilders.*;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class BooleanTest extends AbstractWatchedTest {

    @Test
    public void booleanQueryEmptyTest() {
        int n = cassandraUtils.query(bool()).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void booleanQueryNotTest() {
        int n = cassandraUtils.query(bool().not(match("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51c"))).count();
        assertEquals("Expected 4 result!", 4, n);
    }

    @Test
    public void booleanQueryMustTest() {
        int n = cassandraUtils.query(bool().must(wildcard("ascii_1", "frase*")).must(wildcard("inet_1", "127.0.*")))
                              .count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void booleanQueryShouldTest() {
        int n = cassandraUtils.query(bool().should(wildcard("ascii_1", "frase*")).should(wildcard("inet_1", "127.0.*")))
                              .count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void booleanQueryMustAndNotTest() {
        int n = cassandraUtils.query(bool().must(wildcard("ascii_1", "frase*"))
                                           .must(wildcard("inet_1", "127.0.*"))
                                           .not(match("inet_1", "127.0.0.1"))).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void booleanQueryShouldAndNotTest() {
        int n = cassandraUtils.query(bool().should(wildcard("ascii_1", "frase*"), wildcard("inet_1", "127.0.*"))
                                           .not(match("inet_1", "127.0.0.1"))).count();
        assertEquals("Expected 3 results!", 3, n);
    }

    @Test
    public void booleanQueryMustWithBoostTest() {

        List<Row> rows1 = cassandraUtils.query(bool().must(fuzzy("inet_1", "127.1.1.1").boost(9.9))
                                                     .must(fuzzy("inet_1", "127.1.0.1").boost(0.001))
                                                     .not(match("integer_1", 1)).not(match("integer_1", -4))).get();
        assertEquals("Expected 3 results!", 3, rows1.size());

        List<Row> rows2 = cassandraUtils.query(bool().must(fuzzy("inet_1", "127.1.1.1").boost(0.001))
                                                     .must(fuzzy("inet_1", "127.1.0.1").boost(9.9))
                                                     .not(match("integer_1", 1)).not(match("integer_1", -4))).get();
        assertEquals("Expected 3 results!", 3, rows2.size());

        assertEquals("Expected same number of results ", rows1.size(), rows2.size());

        boolean equals = true;
        for (int i = 0; i < rows1.size(); i++) {
            String firstResult = rows1.get(i).getString(TestingConstants.INDEX_COLUMN_CONSTANT);
            String secondResult = rows2.get(i).getString(TestingConstants.INDEX_COLUMN_CONSTANT);
            equals &= firstResult.equals(secondResult);
        }
        assertFalse("Expected different scoring!", equals);

        equals = true;
        for (int i = 0; i < rows1.size(); i++) {
            Integer firstResult = rows1.get(i).getInt("integer_1");
            Integer secondResult = rows2.get(i).getInt("integer_1");
            equals &= firstResult.equals(secondResult);
        }
        assertFalse("Expected different sorting!", equals);
    }

    @Test
    public void booleanFilterEmptyTest() {
        int n = cassandraUtils.filter(bool()).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void booleanFilterNotTest() {
        int n = cassandraUtils.filter(bool().not(match("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51c"))).count();
        assertEquals("Expected 4 result!", 4, n);
    }

    @Test
    public void booleanFilterMustTest() {
        int n = cassandraUtils.filter(bool().must(wildcard("ascii_1", "frase*")).must(wildcard("inet_1", "127.0.*")))
                              .count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void booleanFilterShouldTest() {
        int n = cassandraUtils.filter(bool().should(wildcard("ascii_1", "frase*"))
                                            .should(wildcard("inet_1", "127.0.*"))).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void booleanFilterMustAndNotTest() {
        int n = cassandraUtils.filter(bool().must(wildcard("ascii_1", "frase*"))
                                            .must(wildcard("inet_1", "127.0.*"))
                                            .not(match("inet_1", "127.0.0.1"))).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void booleanFilterShouldAndNotTest() {
        int n = cassandraUtils.filter(bool().should(wildcard("ascii_1", "frase*"), wildcard("inet_1", "127.0.*"))
                                            .not(match("inet_1", "127.0.0.1"))).count();
        assertEquals("Expected 3 results!", 3, n);
    }

    @Test
    public void booleanFilterMustWithBoostTest() {

        List<Row> firstRows = cassandraUtils.filter(bool().must(fuzzy("inet_1", "127.1.1.1").boost(0.9))
                                                          .must(fuzzy("inet_1", "127.1.0.1").boost(0.1))
                                                          .not(match("integer_1", 1), match("integer_1", -4))).get();
        assertEquals("Expected 3 results!", 3, firstRows.size());

        List<Row> secondRows = cassandraUtils.filter(bool().must(fuzzy("inet_1", "127.1.1.1").boost(0.0))
                                                           .must(fuzzy("inet_1", "127.1.0.1").boost(0.9))
                                                           .not(match("integer_1", 1), match("integer_1", -4))).get();
        assertEquals("Expected 3 results!", 3, secondRows.size());

        assertEquals("Expected same number of results ", firstRows.size(), secondRows.size());
        boolean equals = true;
        for (int i = 0; i < firstRows.size(); i++) {
            Integer firstResult = firstRows.get(i).getInt("integer_1");
            Integer secondResult = secondRows.get(i).getInt("integer_1");
            equals &= firstResult.equals(secondResult);
        }
        assertTrue("Expected same sorting!", equals);
    }

}
