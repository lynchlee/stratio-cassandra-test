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

import com.datastax.driver.core.exceptions.DriverException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.wildcard;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class WildcardTest extends AbstractWatchedTest {

    @Test
    public void wildcardQueryAsciiFieldTest1() {
        int n = cassandraUtils.query(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void wildcardQueryAsciiFieldTest2() {
        int n = cassandraUtils.query(wildcard("ascii_1", "frase*")).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void wildcardQueryAsciiFieldTest3() {
        int n = cassandraUtils.query(wildcard("ascii_1", "frase *")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void wildcardQueryAsciiFieldTest4() {
        int n = cassandraUtils.query(wildcard("ascii_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardQueryInetFieldTest1() {
        int n = cassandraUtils.query(wildcard("inet_1", "*")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void wildcardQueryInetFieldTest2() {
        int n = cassandraUtils.query(wildcard("inet_1", "127*")).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void wildcardQueryInetFieldTest3() {
        int n = cassandraUtils.query(wildcard("inet_1", "127.1.*")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void wildcardQueryInetFieldTest4() {
        int n = cassandraUtils.query(wildcard("inet_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardQueryTextFieldTest1() {
        int n = cassandraUtils.query(wildcard("text_1", "*")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void wildcardQueryTextFieldTest2() {
        int n = cassandraUtils.query(wildcard("text_1", "Frase*")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardQueryTextFieldTest3() {
        int n = cassandraUtils.query(wildcard("text_1", "Frasesin*")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardQueryTextFieldTest4() {
        int n = cassandraUtils.query(wildcard("text_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardQueryVarcharFieldTest1() {
        int n = cassandraUtils.query(wildcard("varchar_1", "*")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void wildcardQueryVarcharFieldTest2() {
        int n = cassandraUtils.query(wildcard("varchar_1", "frase*")).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void wildcardQueryVarcharFieldTest3() {
        int n = cassandraUtils.query(wildcard("varchar_1", "frase sencilla*")).count();
        assertEquals("Expected 1 results!", 1, n);
    }

    @Test
    public void wildcardQueryVarcharFieldTest4() {
        int n = cassandraUtils.query(wildcard("varchar_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardFilterAsciiFieldTest1() {
        int n = cassandraUtils.filter(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void wildcardFilterAsciiFieldTest2() {
        int n = cassandraUtils.filter(wildcard("ascii_1", "frase*")).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void wildcardFilterAsciiFieldTest3() {
        int n = cassandraUtils.filter(wildcard("ascii_1", "frase *")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void wildcardFilterAsciiFieldTest4() {
        int n = cassandraUtils.filter(wildcard("ascii_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardFilterInetFieldTest1() {
        int n = cassandraUtils.filter(wildcard("inet_1", "*")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void wildcardFilterInetFieldTest2() {
        int n = cassandraUtils.filter(wildcard("inet_1", "127*")).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void wildcardFilterInetFieldTest3() {
        int n = cassandraUtils.filter(wildcard("inet_1", "127.1.*")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void wildcardFilterInetFieldTest4() {
        int n = cassandraUtils.filter(wildcard("inet_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardFilterTextFieldTest1() {
        int n = cassandraUtils.filter(wildcard("text_1", "*")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void wildcardFilterTextFieldTest2() {
        int n = cassandraUtils.filter(wildcard("text_1", "Frase*")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardFilterTextFieldTest3() {
        int n = cassandraUtils.filter(wildcard("text_1", "Frasesin*")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardFilterTextFieldTest4() {
        int n = cassandraUtils.filter(wildcard("text_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardFilterVarcharFieldTest1() {
        int n = cassandraUtils.filter(wildcard("varchar_1", "*")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void wildcardFilterVarcharFieldTest2() {
        int n = cassandraUtils.filter(wildcard("varchar_1", "frase*")).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void wildcardFilterVarcharFieldTest3() {
        int n = cassandraUtils.filter(wildcard("varchar_1", "frase sencilla*")).count();
        assertEquals("Expected 1 results!", 1, n);
    }

    @Test
    public void wildcardFilterVarcharFieldTest4() {
        int n = cassandraUtils.filter(wildcard("varchar_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardFilteredQueryTextFieldTest1() {
        int n = cassandraUtils.query(wildcard("varchar_1", "frase*")).filter(wildcard("text_1", "*")).count();
        assertEquals("Expected 5 results!", 4, n);
    }

    @Test
    public void wildcardFilteredQueryTextFieldTest2() {
        int n = cassandraUtils.query(wildcard("text_1", "*")).filter(wildcard("varchar_1", "frase*")).count();
        assertEquals("Expected 5 results!", 4, n);
    }

    @Test
    public void wildcardQueryListFieldTest1() {
        int n = cassandraUtils.query(wildcard("list_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardQueryListFieldTest2() {
        int n = cassandraUtils.query(wildcard("list_1", "l*")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void wildcardQueryListFieldTest3() {
        int n = cassandraUtils.query(wildcard("list_1", "s*")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardQuerySetFieldTest1() {
        int n = cassandraUtils.query(wildcard("set_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardQuerySetFieldTest2() {
        int n = cassandraUtils.query(wildcard("set_1", "l*")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardQuerySetFieldTest3() {
        int n = cassandraUtils.query(wildcard("set_1", "s*")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void wildcardQueryMapFieldTest1() {
        int n = cassandraUtils.query(wildcard("map_1.k1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardQueryMapFieldTest2() {
        int n = cassandraUtils.query(wildcard("map_1.k1", "l*")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardQueryMapFieldTest3() {
        int n = cassandraUtils.query(wildcard("map_1.k1", "k*")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void wildcardQueryMapFieldTest4() {
        int n = cassandraUtils.query(wildcard("map_1.k1", "v*")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

}
