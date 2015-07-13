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

import static com.stratio.cassandra.lucene.search.SearchBuilders.regexp;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class RegexpTest extends AbstractWatchedTest {

    @Test
    public void regexpQueryAsciiFieldTest1() {
        int count = cassandraUtils.query(regexp("ascii_1", "frase.*")).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void regexpQueryAsciiFieldTest2() {
        int count = cassandraUtils.query(regexp("ascii_1", "frase .*")).count();
        assertEquals("Expected 1 result!", 1, count);
    }

    @Test
    public void regexpQueryAsciiFieldTest3() {
        int count = cassandraUtils.query(regexp("ascii_1", ".*")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void regexpQueryAsciiFieldTest4() {
        int count = cassandraUtils.query(regexp("ascii_1", "")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void regexpQueryAsciiFieldTest5() {
        int count = cassandraUtils.query(regexp("ascii_1", "frase tipo ascii")).count();
        assertEquals("Expected 1 result!", 1, count);
    }

    @Test
    public void regexpQueryInetFieldTest1() {
        int count = cassandraUtils.query(regexp("inet_1", ".*")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void regexpQueryInetFieldTest2() {
        int count = cassandraUtils.query(regexp("inet_1", "127.*")).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void regexpQueryInetFieldTest3() {
        int count = cassandraUtils.query(regexp("inet_1", "127.1.*")).count();
        assertEquals("Expected 2 results!", 2, count);
    }

    @Test
    public void regexpQueryInetFieldTest4() {
        int count = cassandraUtils.query(regexp("inet_1", "")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void regexpQueryInetFieldTest5() {
        int count = cassandraUtils.query(regexp("inet_1", "127.1.1.1")).count();
        assertEquals("Expected 2 results!", 2, count);
    }

    @Test
    public void regexpQueryTextFieldTest1() {
        int count = cassandraUtils.query(regexp("text_1", ".*")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void regexpQueryTextFieldTest2() {
        int count = cassandraUtils.query(regexp("text_1", "frase.*")).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void regexpQueryTextFieldTest3() {
        int count = cassandraUtils.query(regexp("text_1", "frase .*")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void regexpQueryTextFieldTest4() {
        int count = cassandraUtils.query(regexp("text_1", "")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void regexpQueryTextFieldTest5() {
        int count = cassandraUtils.query(regexp("text_1", "Frase con espacios articulos y las palabras suficientes"))
                                  .count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void regexpQueryVarcharFieldTest1() {
        int count = cassandraUtils.query(regexp("varchar_1", ".*")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void regexpQueryVarcharFieldTest2() {
        int count = cassandraUtils.query(regexp("varchar_1", "frase.*")).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void regexpQueryVarcharFieldTest3() {
        int count = cassandraUtils.query(regexp("varchar_1", "frase .*")).count();
        assertEquals("Expected 1 results!", 1, count);
    }

    @Test
    public void regexpQueryVarcharFieldTest4() {
        int count = cassandraUtils.query(regexp("varchar_1", "")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void regexpQueryVarcharFieldTest5() {
        int count = cassandraUtils.query(regexp("varchar_1", "frasesencillasinespacios")).count();
        assertEquals("Expected 1 results!", 1, count);
    }

    @Test
    public void regexpQueryListFieldTest1() {
        int n = cassandraUtils.query(regexp("list_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void regexpQueryListFieldTest2() {
        int n = cassandraUtils.query(regexp("list_1", "l.*")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void regexpQueryListFieldTest3() {
        int n = cassandraUtils.query(regexp("list_1", "s.*")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void regexpQuerySetFieldTest1() {
        int n = cassandraUtils.query(regexp("set_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void regexpQuerySetFieldTest2() {
        int n = cassandraUtils.query(regexp("set_1", "l.*")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void regexpQuerySetFieldTest3() {
        int n = cassandraUtils.query(regexp("set_1", "s.*")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void regexpQueryMapFieldTest1() {
        int n = cassandraUtils.query(regexp("map_1.k1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void regexpQueryMapFieldTest2() {
        int n = cassandraUtils.query(regexp("map_1.k1", "l.*")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void regexpQueryMapFieldTest3() {
        int n = cassandraUtils.query(regexp("map_1.k1", "k.*")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void regexpQueryMapFieldTest4() {
        int n = cassandraUtils.query(regexp("map_1.k1", "v.*")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void regexpFilterAsciiFieldTest1() {
        int count = cassandraUtils.filter(regexp("ascii_1", "frase.*")).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void regexpFilterAsciiFieldTest2() {
        int count = cassandraUtils.filter(regexp("ascii_1", "frase .*")).count();
        assertEquals("Expected 1 result!", 1, count);
    }

    @Test
    public void regexpFilterAsciiFieldTest3() {
        int count = cassandraUtils.filter(regexp("ascii_1", ".*")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void regexpFilterAsciiFieldTest4() {
        int count = cassandraUtils.filter(regexp("ascii_1", "")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void regexpFilterAsciiFieldTest5() {
        int count = cassandraUtils.filter(regexp("ascii_1", "frase tipo ascii")).count();
        assertEquals("Expected 1 result!", 1, count);
    }

    @Test
    public void regexpFilterInetFieldTest1() {
        int count = cassandraUtils.filter(regexp("inet_1", ".*")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void regexpFilterInetFieldTest2() {
        int count = cassandraUtils.filter(regexp("inet_1", "127.*")).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void regexpFilterInetFieldTest3() {
        int count = cassandraUtils.filter(regexp("inet_1", "127.1.*")).count();
        assertEquals("Expected 2 results!", 2, count);
    }

    @Test
    public void regexpFilterInetFieldTest4() {
        int count = cassandraUtils.filter(regexp("inet_1", "")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void regexpFilterInetFieldTest5() {
        int count = cassandraUtils.filter(regexp("inet_1", "127.1.1.1")).count();
        assertEquals("Expected 2 results!", 2, count);
    }

    @Test
    public void regexpFilterTextFieldTest1() {
        int count = cassandraUtils.filter(regexp("text_1", ".*")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void regexpFilterTextFieldTest2() {
        int count = cassandraUtils.filter(regexp("text_1", "frase.*")).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void regexpFilterTextFieldTest3() {
        int count = cassandraUtils.filter(regexp("text_1", "frase .*")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void regexpFilterTextFieldTest4() {
        int count = cassandraUtils.filter(regexp("text_1", "")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void regexpFilterTextFieldTest5() {
        int count = cassandraUtils.filter(regexp("text_1", "Frase con espacios articulos y las palabras suficientes"))
                                  .count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void regexpFilterVarcharFieldTest1() {
        int count = cassandraUtils.filter(regexp("varchar_1", ".*")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void regexpFilterVarcharFieldTest2() {
        int count = cassandraUtils.filter(regexp("varchar_1", "frase.*")).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void regexpFilterVarcharFieldTest3() {
        int count = cassandraUtils.filter(regexp("varchar_1", "frase .*")).count();
        assertEquals("Expected 1 results!", 1, count);
    }

    @Test
    public void regexpFilterVarcharFieldTest4() {
        int count = cassandraUtils.filter(regexp("varchar_1", "")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void regexpFilterVarcharFieldTest5() {
        int count = cassandraUtils.filter(regexp("varchar_1", "frasesencillasinespacios")).count();
        assertEquals("Expected 1 results!", 1, count);
    }

    @Test
    public void regexpFilterListFieldTest1() {
        int n = cassandraUtils.filter(regexp("list_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void regexpFilterListFieldTest2() {
        int n = cassandraUtils.filter(regexp("list_1", "l.*")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void regexpFilterListFieldTest3() {
        int n = cassandraUtils.filter(regexp("list_1", "s.*")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void regexpFilterSetFieldTest1() {
        int n = cassandraUtils.filter(regexp("set_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void regexpFilterSetFieldTest2() {
        int n = cassandraUtils.filter(regexp("set_1", "l.*")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void regexpFilterSetFieldTest3() {
        int n = cassandraUtils.filter(regexp("set_1", "s.*")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void regexpFilterMapFieldTest1() {
        int n = cassandraUtils.filter(regexp("map_1.k1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void regexpFilterMapFieldTest2() {
        int n = cassandraUtils.filter(regexp("map_1.k1", "l.*")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void regexpFilterMapFieldTest3() {
        int n = cassandraUtils.filter(regexp("map_1.k1", "k.*")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void regexpFilterMapFieldTest4() {
        int n = cassandraUtils.filter(regexp("map_1.k1", "v.*")).count();
        assertEquals("Expected 2 results!", 2, n);
    }
}
