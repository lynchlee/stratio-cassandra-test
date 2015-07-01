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

/**
 * Created by Jcalderin on 24/03/14.
 */

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.search.SearchBuilders.phrase;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class PhraseTest extends AbstractWatchedTest {

    @Test()
    public void phraseQueryTextFieldTest1() {
        int n = cassandraUtils.query(phrase("text_1", "Frase espacios")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test()
    public void phraseQueryTextFieldWithSlopTest1() {
        int n = cassandraUtils.query(phrase("text_1", "Frase espacios").slop(2)).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test()
    public void phraseQueryTextFieldTest2() {
        int n = cassandraUtils.query(phrase("text_1", "articulos suficientes")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test()
    public void phraseQueryTextFieldWithSlopTest() {
        int n = cassandraUtils.query(phrase("text_1", "articulos palabras").slop(2)).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test()
    public void phraseQueryTextFieldTest3() {
        int n = cassandraUtils.query(phrase("text_1", "con los")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test()
    public void phraseQueryTextFieldTest4() {
        int n = cassandraUtils.query(phrase("text_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void phraseQueryListFieldTest1() {
        int n = cassandraUtils.query(phrase("list_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void phraseQueryListFieldTest2() {
        int n = cassandraUtils.query(phrase("list_1", "l1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void phraseQueryListFieldTest3() {
        int n = cassandraUtils.query(phrase("list_1", "l1 l2")).count();
        assertEquals("Expected 1 results!", 1, n);
    }

    @Test
    public void phraseQueryListFieldTest4() {
        int n = cassandraUtils.query(phrase("list_1", "s1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void phraseQueryListFieldTest5() {
        int n = cassandraUtils.query(phrase("list_1", "l2 l3")).count();
        assertEquals("Expected 3 results!", 3, n);
    }

    @Test
    public void phraseQueryListFieldTest6() {
        int n = cassandraUtils.query(phrase("list_1", "l3 l2")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void phraseQuerySetFieldTest1() {
        int n = cassandraUtils.query(phrase("set_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void phraseQuerySetFieldTest2() {
        int n = cassandraUtils.query(phrase("set_1", "l1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void phraseQuerySetFieldTest3() {
        int n = cassandraUtils.query(phrase("set_1", "s1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void phraseQueryMapFieldTest1() {
        int n = cassandraUtils.query(phrase("map_1.k1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void phraseQueryMapFieldTest2() {
        int n = cassandraUtils.query(phrase("map_1.k1", "l1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void phraseQueryMapFieldTest3() {
        int n = cassandraUtils.query(phrase("map_1.k1", ("k1"))).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void phraseQueryMapFieldTest4() {
        int n = cassandraUtils.query(phrase("map_1.k1", ("v1"))).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test()
    public void phraseFilterTextFieldTest1() {
        int n = cassandraUtils.filter(phrase("text_1", "Frase espacios")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test()
    public void phraseFilterTextFieldWithSlopTest1() {
        int n = cassandraUtils.filter(phrase("text_1", "Frase espacios").slop(2)).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test()
    public void phraseFilterTextFieldTest2() {
        int n = cassandraUtils.filter(phrase("text_1", "articulos suficientes")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test()
    public void phraseFilterTextFieldWithSlopTest() {
        int n = cassandraUtils.filter(phrase("text_1", "articulos palabras").slop(2)).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test()
    public void phraseFilterTextFieldTest3() {
        int n = cassandraUtils.filter(phrase("text_1", "con los")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test()
    public void phraseFilterTextFieldTest4() {
        int n = cassandraUtils.filter(phrase("text_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void phraseFilterListFieldTest1() {
        int n = cassandraUtils.filter(phrase("list_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void phraseFilterListFieldTest2() {
        int n = cassandraUtils.filter(phrase("list_1", "l1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void phraseFilterListFieldTest3() {
        int n = cassandraUtils.filter(phrase("list_1", "l1 l2")).count();
        assertEquals("Expected 1 results!", 1, n);
    }

    @Test
    public void phraseFilterListFieldTest4() {
        int n = cassandraUtils.filter(phrase("list_1", "s1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void phraseFilterListFieldTest5() {
        int n = cassandraUtils.filter(phrase("list_1", "l2 l3")).count();
        assertEquals("Expected 3 results!", 3, n);
    }

    @Test
    public void phraseFilterListFieldTest6() {
        int n = cassandraUtils.filter(phrase("list_1", "l3 l2")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void phraseFilterSetFieldTest1() {
        int n = cassandraUtils.filter(phrase("set_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void phraseFilterSetFieldTest2() {
        int n = cassandraUtils.filter(phrase("set_1", "l1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void phraseFilterSetFieldTest3() {
        int n = cassandraUtils.filter(phrase("set_1", "s1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void phraseFilterMapFieldTest1() {
        int n = cassandraUtils.filter(phrase("map_1.k1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void phraseFilterMapFieldTest2() {
        int n = cassandraUtils.filter(phrase("map_1.k1", "l1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void phraseFilterMapFieldTest3() {
        int n = cassandraUtils.filter(phrase("map_1.k1", ("k1"))).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void phraseFilterMapFieldTest4() {
        int n = cassandraUtils.filter(phrase("map_1.k1", ("v1"))).count();
        assertEquals("Expected 2 results!", 2, n);
    }
}