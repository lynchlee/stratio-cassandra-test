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

import com.datastax.driver.core.exceptions.DriverInternalError;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.index.query.builder.SearchBuilders.fuzzy;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class FuzzyTest extends AbstractWatchedTest {

    @Test
    public void fuzzyQueryAsciiFieldTest() {
        int n = cassandraUtils.query(fuzzy("ascii_1", "frase tipo asci")).count();
        assertEquals("Expected 2 result!", 2, n);
    }

    @Test(expected = DriverInternalError.class)
    public void fuzzyQueryEmptyAsciiFieldTest() {
        cassandraUtils.query(fuzzy("ascii_1", "")).count();
    }

    @Test
    public void fuzzyQueryAsciiFieldWith1MaxEditsTest() {
        int n = cassandraUtils.query(fuzzy("ascii_1", "frase tipo asci").maxEdits(1)).count();

        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void fuzzyQueryAsciiFieldWith0MaxEditsTest() {
        int n = cassandraUtils.query(fuzzy("ascii_1", "frase tipo asci").maxEdits(0)).count();

        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyQueryAsciiFieldWith2PrefixLengthTest1() {
        int n = cassandraUtils.query(fuzzy("ascii_1", "frase typo ascii").prefixLength(2)).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void fuzzyQueryAsciiFieldWith2PrefixLengthTest2() {
        int n = cassandraUtils.query(fuzzy("ascii_1", "phrase tipo ascii").prefixLength(2)).count();
        assertEquals("Expected 0 results!", 0, n);
    }

//    @Test
//    public void fuzzyQueryAsciiFieldWith1MaxExpansionsTest() {
//        int n = cassandraUtils.query(fuzzy("ascii_1", "frase tipo ascii").maxExpansions(1)).count();
//        assertEquals("Expected 1 result!", 1, n);
//    }

    @Test
    public void fuzzyQueryAsciiFieldWith10MaxExpansionsTest() {
        int n = cassandraUtils.query(fuzzy("ascii_1", "frase tipo ascii").maxExpansions(10)).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyQueryAsciiFieldWithoutTranspositionsTest() {
        int n = cassandraUtils.query(fuzzy("ascii_1", "farse itpo ascii").transpositions(false)).count();

        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyQueryAsciiFieldWithTranspositionsTest() {
        int n = cassandraUtils.query(fuzzy("ascii_1", "farse itpo ascii").transpositions(true)).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void fuzzyQueryAsciiFieldWith5MaxEditsAndTranspositionsTest() {
        int n = cassandraUtils.query(fuzzy("ascii_1", "farse itpo ascii").maxEdits(1).transpositions(true)).count();
        assertEquals("Expected 0 results!", 0, n);

    }

    @Test
    public void fuzzyQueryInetFieldTest() {
        int n = cassandraUtils.query(fuzzy("inet_1", "127.0.1.1")).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test(expected = DriverInternalError.class)
    public void fuzzyQueryEmptyInetFieldTest() {
        cassandraUtils.query(fuzzy("inet_1", "")).count();
    }

    @Test
    public void fuzzyQueryInetFieldWith1MaxEditsTest() {
        int n = cassandraUtils.query(fuzzy("inet_1", "127.0.0.1").maxEdits(1)).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyQueryInetFieldWith0MaxEditsTest() {
        int n = cassandraUtils.query(fuzzy("inet_1", "127.0.1.1").maxEdits(0)).count();

        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void fuzzyQueryInetFieldWith2PrefixLengthTest1() {
        int n = cassandraUtils.query(fuzzy("inet_1", "127.0.1.1").prefixLength(2)).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void fuzzyQueryInetFieldWith2PrefixLengthTest2() {
        int n = cassandraUtils.query(fuzzy("inet_1", "117.0.1.1").prefixLength(2)).count();
        assertEquals("Expected 0 results!", 0, n);
    }

//    @Test
//    // 2
//    public void fuzzyQueryInetFieldWith1MaxExpansionsTest() throws InterruptedException {
//        int n = cassandraUtils.query(fuzzy("inet_1", "127.0.1.1").maxExpansions(1)).count();
//        assertEquals("Expected 1 result!", 1, n);
//    }

    @Test
    public void fuzzyQueryInetFieldWith10MaxExpansionsTest() throws InterruptedException {
        int n = cassandraUtils.query(fuzzy("inet_1", "127.0.1.1").maxExpansions(10)).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void fuzzyQueryInetFieldWithoutTranspositionsTest() {
        int n = cassandraUtils.query(fuzzy("inet_1", "1270..1.1").transpositions(false)).count();
        assertEquals("Expected 3 results!", 3, n);
    }

    @Test
    public void fuzzyQueryInetFieldWithTranspositionsTest() {
        int n = cassandraUtils.query(fuzzy("inet_1", "1270..1.1").transpositions(true)).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void fuzzyQueryInetFieldWith1MaxEditsAndTranspositionsTest() {
        int n = cassandraUtils.query(fuzzy("inet_1", "1270..1.1").maxEdits(1).transpositions(true)).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void fuzzyQueryTextFieldTest() {
        int n = cassandraUtils.query(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente")).count();
        assertEquals("Expected 2 result!", 2, n);
    }

    @Test(expected = DriverInternalError.class)
    public void fuzzyQueryEmptyTextFieldTest() {
        cassandraUtils.query(fuzzy("text_1", "")).count();
    }

    @Test
    public void fuzzyQueryTextFieldWith1MaxEditsTest() {

        int n = cassandraUtils.query(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente").maxEdits(1))
                              .count();

        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void fuzzyQueryTextFieldWith0MaxEditsTest() {
        int n = cassandraUtils.query(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente").maxEdits(0))
                              .count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyQueryTextFieldWith2PrefixLengthTest1() {
        int n = cassandraUtils.query(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente").prefixLength(
                2)).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void fuzzyQueryTextFieldWith2PrefixLengthTest2() {
        int n = cassandraUtils.query(fuzzy("text_1", "rFasesinespaciosconarticulosylaspalabrassuficiente").prefixLength(
                2)).count();
        assertEquals("Expected 0 results!", 0, n);
    }

//    @Test
//    // 2
//    public void fuzzyQueryTextFieldWith1MaxExpansionsTest() throws InterruptedException {
//        int n = cassandraUtils.query(fuzzy("text_1",
//                                           "Frasesinespaciosconarticulosylaspalabrassuficiente").maxExpansions(1))
//                              .count();
//        assertEquals("Expected 1 result!", 1, n);
//    }

    @Test
    public void fuzzyQueryTextFieldWith10MaxExpansionsTest() throws InterruptedException {
        int n = cassandraUtils.query(fuzzy("text_1",
                                           "Frasesinespaciosconarticulosylaspalabrassuficiente").maxExpansions(10))
                              .count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyQueryTextFieldWithoutTranspositionsTest() {
        int n = cassandraUtils.query(fuzzy("text_1",
                                           "Frasseinespacisoconarticulosylaspalabrassuficientes").transpositions(false))
                              .count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyQueryTextFieldWithTranspositionsTest() {
        int n = cassandraUtils.query(fuzzy("text_1",
                                           "Frasseinespacisoconarticulosylaspalabrassuficientes").transpositions(true))
                              .count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void fuzzyQueryTextFieldWith5MaxEditsAndTranspositionsTest() {
        int n = cassandraUtils.query(fuzzy("text_1", "Frasseinespacisoconarticulosylaspalabrassuficientes").maxEdits(1)
                                                                                                           .transpositions(
                                                                                                                   true))
                              .count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyQueryVarcharFieldTest() {
        int n = cassandraUtils.query(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga")).count();
        assertEquals("Expected 3 results!", 3, n);
    }

    @Test(expected = DriverInternalError.class)
    public void fuzzyQueryEmptyVarcharFieldTest() {
        cassandraUtils.query(fuzzy("varchar_1", "")).count();
    }

    @Test
    public void fuzzyQueryVarcharFieldWith1MaxEditsTest() {
        int n = cassandraUtils.query(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxEdits(1)).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyQueryVarcharFieldWith0MaxEditsTest() {
        int n = cassandraUtils.query(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxEdits(0)).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyQueryVarcharFieldWith2PrefixLengthTest1() {
        int n = cassandraUtils.query(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").prefixLength(2)).count();
        assertEquals("Expected 2 result2!", 2, n);
    }

    @Test
    public void fuzzyQueryVarcharFieldWith2PrefixLengthTest2() {
        int n = cassandraUtils.query(fuzzy("varchar_1", "rfasesencillasnespaciosperomaslarga").prefixLength(2)).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyQueryVarcharFieldWith1MaxExpansionsTest() throws InterruptedException {
        int n = cassandraUtils.query(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxExpansions(1))
                              .count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyQueryVarcharFieldWith10MaxExpansionsTest() throws InterruptedException {
        int n = cassandraUtils.query(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxExpansions(10))
                              .count();
        assertEquals("Expected 3 results!", 3, n);
    }

    @Test
    public void fuzzyQueryVarcharFieldWithoutTranspositionsTest() {
        int n = cassandraUtils.query(fuzzy("varchar_1", "frasesenicllasnespaciosperomaslarga").transpositions(false))
                              .count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyQueryVarcharFieldWithTranspositionsTest() {
        int n = cassandraUtils.query(fuzzy("varchar_1", "frasesenicllasnespaciosperomaslarga").transpositions(true))
                              .count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyQueryVarcharFieldWith5MaxEditsAndTranspositionsTest() {
        int n = cassandraUtils.query(fuzzy("varchar_1", "frasesenicllasnespaciosperomaslarga").maxEdits(1)
                                                                                              .transpositions(true))
                              .count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test(expected = DriverInternalError.class)
    public void fuzzyQueryListFieldTest1() {
        int n = cassandraUtils.query(fuzzy("list_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyQueryListFieldTest2() {
        int n = cassandraUtils.query(fuzzy("list_1", "l1")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void fuzzyQueryListFieldTest3() {
        int n = cassandraUtils.query(fuzzy("list_1", "s1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyQueryListFieldTest4() {
        int n = cassandraUtils.query(fuzzy("list_1", "s7l")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test(expected = DriverInternalError.class)
    public void fuzzyQuerySetFieldTest1() {
        int n = cassandraUtils.query(fuzzy("set_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyQuerySetFieldTest2() {
        int n = cassandraUtils.query(fuzzy("set_1", "l1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyQuerySetFieldTest3() {
        int n = cassandraUtils.query(fuzzy("set_1", "s1")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void fuzzyQuerySetFieldTest4() {
        int n = cassandraUtils.query(fuzzy("set_1", "k87")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test(expected = DriverInternalError.class)
    public void fuzzyQueryMapFieldTest1() {
        int n = cassandraUtils.query(fuzzy("map_1.k1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyQueryMapFieldTest2() {
        int n = cassandraUtils.query(fuzzy("map_1.k1", "l1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyQueryMapFieldTest3() {
        int n = cassandraUtils.query(fuzzy("map_1.k1", "k1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyQueryMapFieldTest4() {
        int n = cassandraUtils.query(fuzzy("map_1.k1", "v1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyFilterAsciiFieldTest() {
        int n = cassandraUtils.filter(fuzzy("ascii_1", "frase tipo asci")).count();
        assertEquals("Expected 2 result!", 2, n);
    }

    @Test(expected = DriverInternalError.class)
    public void fuzzyFilterEmptyAsciiFieldTest() {
        cassandraUtils.filter(fuzzy("ascii_1", "")).count();
    }

    @Test
    public void fuzzyFilterAsciiFieldWith1MaxEditsTest() {
        int n = cassandraUtils.filter(fuzzy("ascii_1", "frase tipo asci").maxEdits(1)).count();

        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void fuzzyFilterAsciiFieldWith0MaxEditsTest() {
        int n = cassandraUtils.filter(fuzzy("ascii_1", "frase tipo asci").maxEdits(0)).count();

        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyFilterAsciiFieldWith2PrefixLengthTest1() {
        int n = cassandraUtils.filter(fuzzy("ascii_1", "frase typo ascii").prefixLength(2)).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void fuzzyFilterAsciiFieldWith2PrefixLengthTest2() {
        int n = cassandraUtils.filter(fuzzy("ascii_1", "phrase tipo ascii").prefixLength(2)).count();
        assertEquals("Expected 0 results!", 0, n);
    }

//    @Test
//    // 2
//    public void fuzzyFilterAsciiFieldWith1MaxExpansionsTest() {
//        int n = cassandraUtils.filter(fuzzy("ascii_1", "frase tipo ascii").maxExpansions(1)).count();
//        assertEquals("Expected 1 result!", 1, n);
//    }

    @Test
    public void fuzzyFilterAsciiFieldWith10MaxExpansionsTest() {
        int n = cassandraUtils.filter(fuzzy("ascii_1", "frase tipo ascii").maxExpansions(10)).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyFilterAsciiFieldWithoutTranspositionsTest() {
        int n = cassandraUtils.filter(fuzzy("ascii_1", "farse itpo ascii").transpositions(false)).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyFilterAsciiFieldWithTranspositionsTest() {
        int n = cassandraUtils.filter(fuzzy("ascii_1", "farse itpo ascii").transpositions(true)).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void fuzzyFilterAsciiFieldWith5MaxEditsAndTranspositionsTest() {
        int n = cassandraUtils.filter(fuzzy("ascii_1", "farse itpo ascii").maxEdits(1).transpositions(true)).count();
        assertEquals("Expected 0 results!", 0, n);

    }

    @Test
    public void fuzzyFilterInetFieldTest() {
        int n = cassandraUtils.filter(fuzzy("inet_1", "127.0.1.1")).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test(expected = DriverInternalError.class)
    public void fuzzyFilterEmptyInetFieldTest() {
        cassandraUtils.filter(fuzzy("inet_1", "")).count();
    }

    @Test
    public void fuzzyFilterInetFieldWith1MaxEditsTest() {
        int n = cassandraUtils.filter(fuzzy("inet_1", "127.0.0.1").maxEdits(1)).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyFilterInetFieldWith0MaxEditsTest() {
        int n = cassandraUtils.filter(fuzzy("inet_1", "127.0.1.1").maxEdits(0)).count();

        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void fuzzyFilterInetFieldWith2PrefixLengthTest1() {
        int n = cassandraUtils.filter(fuzzy("inet_1", "127.0.1.1").prefixLength(2)).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void fuzzyFilterInetFieldWith2PrefixLengthTest2() {
        int n = cassandraUtils.filter(fuzzy("inet_1", "117.0.1.1").prefixLength(2)).count();
        assertEquals("Expected 0 results!", 0, n);
    }

//    @Test
//    // 4
//    public void fuzzyFilterInetFieldWith1MaxExpansionsTest() throws InterruptedException {
//        int n = cassandraUtils.filter(fuzzy("inet_1", "127.0.1.1").maxExpansions(1)).count();
//        assertEquals("Expected 1 result!", 1, n);
//    }

    @Test
    public void fuzzyFilterInetFieldWith10MaxExpansionsTest() throws InterruptedException {
        int n = cassandraUtils.filter(fuzzy("inet_1", "127.0.1.1").maxExpansions(10)).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void fuzzyFilterInetFieldWithoutTranspositionsTest() {
        int n = cassandraUtils.filter(fuzzy("inet_1", "1270..1.1").transpositions(false)).count();
        assertEquals("Expected 3 results!", 3, n);
    }

    @Test
    public void fuzzyFilterInetFieldWithTranspositionsTest() {
        int n = cassandraUtils.filter(fuzzy("inet_1", "1270..1.1").transpositions(true)).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void fuzzyFilterInetFieldWith1MaxEditsAndTranspositionsTest() {
        int n = cassandraUtils.filter(fuzzy("inet_1", "1270..1.1").maxEdits(1).transpositions(true)).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void fuzzyFilterTextFieldTest() {
        int n = cassandraUtils.filter(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente")).count();
        assertEquals("Expected 2 result!", 2, n);
    }

    @Test(expected = DriverInternalError.class)
    public void fuzzyFilterEmptyTextFieldTest() {
        cassandraUtils.filter(fuzzy("text_1", "")).count();
    }

    @Test
    public void fuzzyFilterTextFieldWith1MaxEditsTest() {

        int n = cassandraUtils.filter(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente").maxEdits(1))
                              .count();

        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void fuzzyFilterTextFieldWith0MaxEditsTest() {
        int n = cassandraUtils.filter(fuzzy("text_1", "Frasesinespaciosconarticulosylaspalabrassuficiente").maxEdits(0))
                              .count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyFilterTextFieldWith2PrefixLengthTest1() {
        int n = cassandraUtils.filter(fuzzy("text_1",
                                            "Frasesinespaciosconarticulosylaspalabrassuficiente").prefixLength(2))
                              .count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void fuzzyFilterTextFieldWith2PrefixLengthTest2() {
        int n = cassandraUtils.filter(fuzzy("text_1",
                                            "rFasesinespaciosconarticulosylaspalabrassuficiente").prefixLength(2))
                              .count();
        assertEquals("Expected 0 results!", 0, n);
    }

//    @Test
//    // 2
//    public void fuzzyFilterTextFieldWith1MaxExpansionsTest() throws InterruptedException {
//        int n = cassandraUtils.filter(fuzzy("text_1",
//                                            "Frasesinespaciosconarticulosylaspalabrassuficiente").maxExpansions(1))
//                              .count();
//        assertEquals("Expected 1 result!", 1, n);
//    }

    @Test
    public void fuzzyFilterTextFieldWith10MaxExpansionsTest() throws InterruptedException {
        int n = cassandraUtils.filter(fuzzy("text_1",
                                            "Frasesinespaciosconarticulosylaspalabrassuficiente").maxExpansions(10))
                              .count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyFilterTextFieldWithoutTranspositionsTest() {
        int n = cassandraUtils.filter(fuzzy("text_1",
                                            "Frasseinespacisoconarticulosylaspalabrassuficientes").transpositions(false))
                              .count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyFilterTextFieldWithTranspositionsTest() {
        int n = cassandraUtils.filter(fuzzy("text_1",
                                            "Frasseinespacisoconarticulosylaspalabrassuficientes").transpositions(true))
                              .count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void fuzzyFilterTextFieldWith5MaxEditsAndTranspositionsTest() {
        int n = cassandraUtils.filter(fuzzy("text_1", "Frasseinespacisoconarticulosylaspalabrassuficientes").maxEdits(1)
                                                                                                            .transpositions(
                                                                                                                    true))
                              .count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyFilterVarcharFieldTest() {
        int n = cassandraUtils.filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga")).count();
        assertEquals("Expected 3 results!", 3, n);
    }

    @Test(expected = DriverInternalError.class)
    public void fuzzyFilterEmptyVarcharFieldTest() {
        cassandraUtils.filter(fuzzy("varchar_1", "")).count();
    }

    @Test
    public void fuzzyFilterVarcharFieldWith1MaxEditsTest() {
        int n = cassandraUtils.filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxEdits(1)).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyFilterVarcharFieldWith0MaxEditsTest() {
        int n = cassandraUtils.filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxEdits(0)).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyFilterVarcharFieldWith2PrefixLengthTest1() {
        int n = cassandraUtils.filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").prefixLength(2))
                              .count();
        assertEquals("Expected 2 result2!", 2, n);
    }

    @Test
    public void fuzzyFilterVarcharFieldWith2PrefixLengthTest2() {
        int n = cassandraUtils.filter(fuzzy("varchar_1", "rfasesencillasnespaciosperomaslarga").prefixLength(2))
                              .count();
        assertEquals("Expected 0 results!", 0, n);
    }

//    @Test
//    // 3
//    public void fuzzyFilterVarcharFieldWith1MaxExpansionsTest() throws InterruptedException {
//        int n = cassandraUtils.filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxExpansions(1))
//                              .count();
//        assertEquals("Expected 2 results!", 2, n);
//    }

    @Test
    public void fuzzyFilterVarcharFieldWith10MaxExpansionsTest() throws InterruptedException {
        int n = cassandraUtils.filter(fuzzy("varchar_1", "frasesencillasnespaciosperomaslarga").maxExpansions(10))
                              .count();
        assertEquals("Expected 3 results!", 3, n);
    }

    @Test
    public void fuzzyFilterVarcharFieldWithoutTranspositionsTest() {
        int n = cassandraUtils.filter(fuzzy("varchar_1", "frasesenicllasnespaciosperomaslarga").transpositions(false))
                              .count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyFilterVarcharFieldWithTranspositionsTest() {
        int n = cassandraUtils.filter(fuzzy("varchar_1", "frasesenicllasnespaciosperomaslarga").transpositions(true))
                              .count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyFilterVarcharFieldWith5MaxEditsAndTranspositionsTest() {
        int n = cassandraUtils.filter(fuzzy("varchar_1", "frasesenicllasnespaciosperomaslarga").maxEdits(1)
                                                                                               .transpositions(true))
                              .count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test(expected = DriverInternalError.class)
    public void fuzzyFilterListFieldTest1() {
        int n = cassandraUtils.filter(fuzzy("list_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyFilterListFieldTest2() {
        int n = cassandraUtils.filter(fuzzy("list_1", "l1")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void fuzzyFilterListFieldTest3() {
        int n = cassandraUtils.filter(fuzzy("list_1", "s1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyFilterListFieldTest4() {
        int n = cassandraUtils.filter(fuzzy("list_1", "s7l")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test(expected = DriverInternalError.class)
    public void fuzzyFilterSetFieldTest1() {
        int n = cassandraUtils.filter(fuzzy("set_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyFilterSetFieldTest2() {
        int n = cassandraUtils.filter(fuzzy("set_1", "l1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyFilterSetFieldTest3() {
        int n = cassandraUtils.filter(fuzzy("set_1", "s1")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void fuzzyFilterSetFieldTest4() {
        int n = cassandraUtils.filter(fuzzy("set_1", "k87")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test(expected = DriverInternalError.class)
    public void fuzzyFilterMapFieldTest1() {
        int n = cassandraUtils.filter(fuzzy("map_1.k1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void fuzzyFilterMapFieldTest2() {
        int n = cassandraUtils.filter(fuzzy("map_1.k1", "l1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyFilterMapFieldTest3() {
        int n = cassandraUtils.filter(fuzzy("map_1.k1", "k1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void fuzzyFilterMapFieldTest4() {
        int n = cassandraUtils.filter(fuzzy("map_1.k1", "v1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }
}
