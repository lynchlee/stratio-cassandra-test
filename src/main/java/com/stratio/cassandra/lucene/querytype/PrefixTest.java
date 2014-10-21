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

import static com.stratio.cassandra.index.query.builder.SearchBuilders.prefix;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class PrefixTest extends AbstractWatchedTest {

    @Test
    public void prefixQueryAsciiFieldTest1() {
        int n = cassandraUtils.query(prefix("ascii_1", "frase ")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void prefixQueryAsciiFieldTest2() {
        int n = cassandraUtils.query(prefix("ascii_1", "frase")).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void prefixQueryAsciiFieldTest3() {
        int n = cassandraUtils.query(prefix("ascii_1", "F")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void prefixQueryAsciiFieldTest4() {
        int n = cassandraUtils.query(prefix("ascii_1", "")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void prefixQueryInetFieldTest1() {
        int n = cassandraUtils.query(prefix("inet_1", "127")).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void prefixQueryInetFieldTest2() {
        int n = cassandraUtils.query(prefix("inet_1", "")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void prefixQueryInetFieldTest3() {
        int n = cassandraUtils.query(prefix("inet_1", "127.0.")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void prefixQueryTextFieldTest1() {
        int n = cassandraUtils.query(prefix("text_1", "Frase con espacios articulos y las palabras suficientes"))
                              .count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void prefixQueryTextFieldTest2() {
        int n = cassandraUtils.query(prefix("text_1", "Frase")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void prefixQueryTextFieldTest3() {
        int n = cassandraUtils.query(prefix("text_1", "")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void prefixQueryVarcharFieldTest1() {
        int n = cassandraUtils.query(prefix("varchar_1", "frasesencillasinespaciosperomaslarga")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void prefixQueryVarcharFieldTest2() {
        int n = cassandraUtils.query(prefix("varchar_1", "frase")).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void prefixQueryVarcharFieldTest3() {
        int n = cassandraUtils.query(prefix("varchar_1", "")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void prefixQueryListFieldTest1() {
        int n = cassandraUtils.query(prefix("list_1", "")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void prefixQueryListFieldTest2() {
        int n = cassandraUtils.query(prefix("list_1", "l1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void prefixQueryListFieldTest3() {
        int n = cassandraUtils.query(prefix("list_1", "l")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void prefixQueryListFieldTest4() {
        int n = cassandraUtils.query(prefix("list_1", "s1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void prefixQuerySetFieldTest1() {
        int n = cassandraUtils.query(prefix("set_1", "")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void prefixQuerySetFieldTest2() {
        int n = cassandraUtils.query(prefix("set_1", "l1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void prefixQuerySetFieldTest3() {
        int n = cassandraUtils.query(prefix("set_1", "s1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void prefixQueryMapFieldTest1() {
        int n = cassandraUtils.query(prefix("map_1.k1", "")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void prefixQueryMapFieldTest2() {
        int n = cassandraUtils.query(prefix("map_1.k1", "l1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void prefixQueryMapFieldTest3() {
        int n = cassandraUtils.query(prefix("map_1.k1", "k1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void prefixQueryMapFieldTest4() {
        int n = cassandraUtils.query(prefix("map_1.k1", "v1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void prefixFilterAsciiFieldTest1() {
        int n = cassandraUtils.filter(prefix("ascii_1", "frase ")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void prefixFilterAsciiFieldTest2() {
        int n = cassandraUtils.filter(prefix("ascii_1", "frase")).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void prefixFilterAsciiFieldTest3() {
        int n = cassandraUtils.filter(prefix("ascii_1", "F")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void prefixFilterAsciiFieldTest4() {
        int n = cassandraUtils.filter(prefix("ascii_1", "")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void prefixFilterInetFieldTest1() {
        int n = cassandraUtils.filter(prefix("inet_1", "127")).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void prefixFilterInetFieldTest2() {
        int n = cassandraUtils.filter(prefix("inet_1", "")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void prefixFilterInetFieldTest3() {
        int n = cassandraUtils.filter(prefix("inet_1", "127.0.")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void prefixFilterTextFieldTest1() {
        int n = cassandraUtils.filter(prefix("text_1", "Frase con espacios articulos y las palabras suficientes"))
                              .count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void prefixFilterTextFieldTest2() {
        int n = cassandraUtils.filter(prefix("text_1", "Frase")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void prefixFilterTextFieldTest3() {
        int n = cassandraUtils.filter(prefix("text_1", "")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void prefixFilterVarcharFieldTest1() {
        int n = cassandraUtils.filter(prefix("varchar_1", "frasesencillasinespaciosperomaslarga")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void prefixFilterVarcharFieldTest2() {
        int n = cassandraUtils.filter(prefix("varchar_1", "frase")).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void prefixFilterVarcharFieldTest3() {
        int n = cassandraUtils.filter(prefix("varchar_1", "")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void prefixFilterListFieldTest1() {
        int n = cassandraUtils.filter(prefix("list_1", "")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void prefixFilterListFieldTest2() {
        int n = cassandraUtils.filter(prefix("list_1", "l1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void prefixFilterListFieldTest3() {
        int n = cassandraUtils.filter(prefix("list_1", "l")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void prefixFilterListFieldTest4() {
        int n = cassandraUtils.filter(prefix("list_1", "s1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void prefixFilterSetFieldTest1() {
        int n = cassandraUtils.filter(prefix("set_1", "")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void prefixFilterSetFieldTest2() {
        int n = cassandraUtils.filter(prefix("set_1", "l1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void prefixFilterSetFieldTest3() {
        int n = cassandraUtils.filter(prefix("set_1", "s1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void prefixFilterMapFieldTest1() {
        int n = cassandraUtils.filter(prefix("map_1.k1", "")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void prefixFilterMapFieldTest2() {
        int n = cassandraUtils.filter(prefix("map_1.k1", "l1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void prefixFilterMapFieldTest3() {
        int n = cassandraUtils.filter(prefix("map_1.k1", "k1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void prefixFilterMapFieldTest4() {
        int n = cassandraUtils.filter(prefix("map_1.k1", "v1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }
}
