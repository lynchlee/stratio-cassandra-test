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
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.range;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class RangeTest extends AbstractWatchedTest {

    @Test
    public void rangeQueryAsciiFieldTest1() {
        int count = cassandraUtils.query(range("ascii_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeQueryAsciiFieldTest2() {
        int count = cassandraUtils.query(range("ascii_1").lower("a").upper("g")).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeQueryAsciiFieldTest3() {
        int count = cassandraUtils.query(range("ascii_1").lower("a").upper("b")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryAsciiFieldTest4() {
        int count = cassandraUtils.query(range("ascii_1").lower("a").upper("f")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryAsciiFieldTest5() {
        int count = cassandraUtils.query(range("ascii_1").lower("a").upper("f").includeLower(true).includeUpper(true))
                                  .count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryIntegerTest1() {
        int count = cassandraUtils.query(range("integer_1").lower("-5").upper("5")).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeQueryIntegerTest2() {
        int count = cassandraUtils.query(range("integer_1").lower("-4").upper("4")).count();
        assertEquals("Expected 3 results!", 3, count);
    }

    @Test
    public void rangeQueryIntegerTest3() {
        int count = cassandraUtils.query(range("integer_1").lower("-4")
                                                           .upper("-1")
                                                           .includeLower(true)
                                                           .includeUpper(true)).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeQueryIntegerTest4() {
        int count = cassandraUtils.query(range("integer_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeQueryBigintTest1() {
        int count = cassandraUtils.query(range("bigint_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeQueryBigintTest2() {
        int count = cassandraUtils.query(range("bigint_1").lower("999999999999999").upper("1000000000000001")).count();
        assertEquals("Expected 1 result!", 1, count);
    }

    @Test
    public void rangeQueryBigintTest3() {
        int count = cassandraUtils.query(range("bigint_1").lower("1000000000000000").upper("2000000000000000")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryBigintTest4() {
        int count = cassandraUtils.query(range("bigint_1").lower("1000000000000000")
                                                          .upper("2000000000000000")
                                                          .includeLower(true)
                                                          .includeUpper(true)).count();
        assertEquals("Expected 2 results!", 2, count);
    }

    @Test
    public void rangeQueryBigintTest5() {
        int count = cassandraUtils.query(range("bigint_1").lower("1").upper("3").includeLower(true).includeUpper(true))
                                  .count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryBlobTest1() {
        int count = cassandraUtils.query(range("blob_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeQueryBlobTest2() {
        int count = cassandraUtils.query(range("blob_1").lower("0x3E0A15").upper("0x3E0A17")).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeQueryBlobTest3() {
        int count = cassandraUtils.query(range("blob_1").lower("0x3E0A16").upper("0x3E0A17")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryBlobTest4() {
        int count = cassandraUtils.query(range("blob_1").lower("0x3E0A16")
                                                        .upper("0x3E0A17")
                                                        .includeLower(true)
                                                        .includeUpper(true)).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeQueryBlobTest5() {
        int count = cassandraUtils.query(range("blob_1").lower("0x3E0A17")
                                                        .upper("0x3E0A18")
                                                        .includeLower(true)
                                                        .includeUpper(true)).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryBooleanTest1() {
        int count = cassandraUtils.query(range("boolean_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeQueryDecimalTest1() {
        int count = cassandraUtils.query(range("decimal_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeQueryDecimalTest2() {
        int count = cassandraUtils.query(range("decimal_1").lower("1999999999.9").upper("2000000000.1")).count();
        assertEquals("Expected 1 result!", 1, count);
    }

    @Test
    public void rangeQueryDecimalTest3() {
        int count = cassandraUtils.query(range("decimal_1").lower("2000000000.0").upper("3000000000.0")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryDecimalTest4() {
        int count = cassandraUtils.query(range("decimal_1").lower("2000000000.0")
                                                           .upper("3000000000.0")
                                                           .includeLower(true)
                                                           .includeUpper(true)).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeQueryDecimalTest5() {
        int count = cassandraUtils.query(range("decimal_1").lower("2000000000.000001").upper("2000000000.181235"))
                                  .count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryDoubleTest1() {
        int count = cassandraUtils.query(range("double_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeQueryDoubleTest2() {
        int count = cassandraUtils.query(range("double_1").lower("1.9").upper("2.1")).count();
        assertEquals("Expected 1 result!", 1, count);
    }

    @Test
    public void rangeQueryDoubleTest3() {
        int count = cassandraUtils.query(range("double_1").lower("2.0").upper("3.0")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryDoubleTest4() {
        int count = cassandraUtils.query(range("double_1").lower("2.0")
                                                          .upper("3.0")
                                                          .includeLower(true)
                                                          .includeUpper(true)).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeQueryDoubleTest5() {
        int count = cassandraUtils.query(range("double_1").lower("7.0")
                                                          .upper("10.0")
                                                          .includeLower(true)
                                                          .includeUpper(true)).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryFloatTest1() {
        int count = cassandraUtils.query(range("float_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeQueryFloatTest2() {
        int count = cassandraUtils.query(range("float_1").lower("1.9").upper("2.1")).count();
        assertEquals("Expected 1 result!", 1, count);
    }

    @Test
    public void rangeQueryFloatTest3() {
        int count = cassandraUtils.query(range("float_1").lower("1.0").upper("2.0")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryFloatTest4() {
        int count = cassandraUtils.query(range("float_1").lower("1.0")
                                                         .upper("2.0")
                                                         .includeLower(true)
                                                         .includeUpper(true)).count();
        assertEquals("Expected 2 results!", 2, count);
    }

    @Test
    public void rangeQueryFloatTest5() {
        int count = cassandraUtils.query(range("float_1").lower("7.0").upper("9.0")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryUuidTest1() {
        int count = cassandraUtils.query(range("uuid_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test(expected = InvalidQueryException.class)
    public void rangeQueryUuidTest2() {
        cassandraUtils.query(range("uuid_1").lower("1").upper("9")).count();
    }

    @Test
    public void rangeQueryUuidTest3() {
        int count = cassandraUtils.query(range("uuid_1").lower("60297440-b4fa-11e3-8b5a-0002a5d5c51c")
                                                        .upper("60297440-b4fa-11e3-8b5a-0002a5d5c51d")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryUuidTest4() {
        int count = cassandraUtils.query(range("uuid_1").lower("60297440-b4fa-11e3-8b5a-0002a5d5c51c")
                                                        .upper("60297440-b4fa-11e3-8b5a-0002a5d5c51d")
                                                        .includeLower(true)
                                                        .includeUpper(true)).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeQueryTimeuuidTest1() {
        int count = cassandraUtils.query(range("timeuuid_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test(expected = InvalidQueryException.class)
    public void rangeQueryTimeuuidTest2() {
        cassandraUtils.query(range("timeuuid_1").lower("a").upper("z")).count();
    }

    @Test
    public void rangeQueryTimeuuidTest3() {
        int count = cassandraUtils.query(range("timeuuid_1").lower("a4a70900-24e1-11df-8924-001ff3591712")
                                                            .upper("a4a70900-24e1-11df-8924-001ff3591713")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryTimeuuidTest4() {
        int count = cassandraUtils.query(range("timeuuid_1").lower("a4a70900-24e1-11df-8924-001ff3591712")
                                                            .upper("a4a70900-24e1-11df-8924-001ff3591713")
                                                            .includeLower(true)
                                                            .includeUpper(true)).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeQueryInetFieldTest1() {
        int count = cassandraUtils.query(range("inet_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeQueryInetFieldTest2() {
        int count = cassandraUtils.query(range("inet_1").lower("127.0.0.0").upper("127.1.0.0")).count();
        assertEquals("Expected 2 results!", 2, count);
    }

    @Test
    public void rangeQueryInetFieldTest3() {
        int count = cassandraUtils.query(range("inet_1").lower("127.0.0.0")
                                                        .upper("127.1.0.0")
                                                        .includeLower(true)
                                                        .includeUpper(true)).count();
        assertEquals("Expected 2 results!", 2, count);
    }

    @Test
    public void rangeQueryInetFieldTest4() {
        int count = cassandraUtils.query(range("inet_1").lower("192.168.0.0").upper("192.168.0.1")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryTextFieldTest1() {

        int count = cassandraUtils.query(range("text_1")).count();

        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeQueryTextFieldTest2() {
        int count = cassandraUtils.query(range("text_1").lower("frase").upper("g")).count();
        assertEquals("Expected 3 results!", 3, count);
    }

    @Test
    public void rangeQueryTextFieldTest3() {
        int count = cassandraUtils.query(range("text_1").lower(
                "frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga").upper("g")).count();
        assertEquals("Expected 1 result!", 1, count);
    }

    @Test
    public void rangeQueryTextFieldTest4() {
        int count = cassandraUtils.query(range("text_1").lower(
                "frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga")
                                                        .upper("g")
                                                        .includeLower(true)
                                                        .includeUpper(true)).count();
        assertEquals("Expected 2 results!", 2, count);
    }

    @Test
    public void rangeQueryTextFieldTest5() {
        int count = cassandraUtils.query(range("text_1").lower("G").upper("H").includeLower(true).includeUpper(true))
                                  .count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryVarcharFieldTest1() {
        int count = cassandraUtils.query(range("varchar_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeQueryVarcharFieldTest2() {
        int count = cassandraUtils.query(range("varchar_1").lower("frase").upper("g")).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeQueryVarcharFieldTest3() {
        int count = cassandraUtils.query(range("varchar_1").lower("frasesencillasinespaciosperomaslarga").upper("g"))
                                  .count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryVarcharFieldTest4() {
        int count = cassandraUtils.query(range("varchar_1").lower("frasesencillasinespaciosperomaslarga")
                                                           .upper("gH")
                                                           .includeLower(true)
                                                           .includeUpper(true)).count();
        assertEquals("Expected 2 results!", 2, count);
    }

    @Test
    public void rangeQueryVarcharFieldTest5() {
        int count = cassandraUtils.query(range("varchar_1").lower("g").upper("h")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeQueryListFieldTest1() {
        int n = cassandraUtils.query(range("list_1").lower("a").upper("z")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void rangeQueryListFieldTest2() {
        int n = cassandraUtils.query(range("list_1").lower("a1").upper("z9")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void rangeQueryListFieldTest3() {
        int n = cassandraUtils.query(range("list_1").lower("a2").upper("l1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void rangeQuerySetFieldTest1() {
        int n = cassandraUtils.query(range("set_1").lower("a").upper("z")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void rangeQuerySetFieldTest2() {
        int n = cassandraUtils.query(range("set_1").lower("a1").upper("z9")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void rangeQuerySetFieldTest3() {
        int n = cassandraUtils.query(range("set_1").lower("a1").upper("z1")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void rangeQueryMapFieldTest1() {
        int n = cassandraUtils.query(range("map_1.k1").lower("a").upper("z")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void rangeQueryMapFieldTest2() {
        int n = cassandraUtils.query(range("map_1.k1").lower("a1").upper("z9")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void rangeQueryMapFieldTest3() {
        int n = cassandraUtils.query(range("map_1.k1").lower("a1").upper("k9")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void rangeQueryMapFieldTest4() {
        int n = cassandraUtils.query(range("map_1.k1").lower("a1").upper("k1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void rangeFilterAsciiFieldTest1() {
        int count = cassandraUtils.filter(range("ascii_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeFilterAsciiFieldTest2() {
        int count = cassandraUtils.filter(range("ascii_1").lower("a").upper("g")).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeFilterAsciiFieldTest3() {
        int count = cassandraUtils.filter(range("ascii_1").lower("a").upper("b")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterAsciiFieldTest4() {
        int count = cassandraUtils.filter(range("ascii_1").lower("a").upper("f")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterAsciiFieldTest5() {
        int count = cassandraUtils.filter(range("ascii_1").lower("a").upper("f").includeLower(true).includeUpper(true))
                                  .count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterIntegerTest1() {
        int count = cassandraUtils.filter(range("integer_1").lower("-5").upper("5")).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeFilterIntegerTest2() {
        int count = cassandraUtils.filter(range("integer_1").lower("-4").upper("4")).count();
        assertEquals("Expected 3 results!", 3, count);
    }

    @Test
    public void rangeFilterIntegerTest3() {
        int count = cassandraUtils.filter(range("integer_1").lower("-4")
                                                            .upper("-1")
                                                            .includeLower(true)
                                                            .includeUpper(true)).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeFilterIntegerTest4() {
        int count = cassandraUtils.filter(range("integer_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeFilterBigintTest1() {
        int count = cassandraUtils.filter(range("bigint_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeFilterBigintTest2() {
        int count = cassandraUtils.filter(range("bigint_1").lower("999999999999999").upper("1000000000000001")).count();
        assertEquals("Expected 1 result!", 1, count);
    }

    @Test
    public void rangeFilterBigintTest3() {
        int count = cassandraUtils.filter(range("bigint_1").lower("1000000000000000").upper("2000000000000000"))
                                  .count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterBigintTest4() {
        int count = cassandraUtils.filter(range("bigint_1").lower("1000000000000000")
                                                           .upper("2000000000000000")
                                                           .includeLower(true)
                                                           .includeUpper(true)).count();
        assertEquals("Expected 2 results!", 2, count);
    }

    @Test
    public void rangeFilterBigintTest5() {
        int count = cassandraUtils.filter(range("bigint_1").lower("1").upper("3").includeLower(true).includeUpper(true))
                                  .count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterBlobTest1() {
        int count = cassandraUtils.filter(range("blob_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeFilterBlobTest2() {
        int count = cassandraUtils.filter(range("blob_1").lower("0x3E0A15").upper("0x3E0A17")).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeFilterBlobTest3() {
        int count = cassandraUtils.filter(range("blob_1").lower("0x3E0A16").upper("0x3E0A17")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterBlobTest4() {
        int count = cassandraUtils.filter(range("blob_1").lower("0x3E0A16")
                                                         .upper("0x3E0A17")
                                                         .includeLower(true)
                                                         .includeUpper(true)).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeFilterBlobTest5() {
        int count = cassandraUtils.filter(range("blob_1").lower("0x3E0A17")
                                                         .upper("0x3E0A18")
                                                         .includeLower(true)
                                                         .includeUpper(true)).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterBooleanTest1() {
        int count = cassandraUtils.filter(range("boolean_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeFilterDecimalTest1() {
        int count = cassandraUtils.filter(range("decimal_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeFilterDecimalTest2() {
        int count = cassandraUtils.filter(range("decimal_1").lower("1999999999.9").upper("2000000000.1")).count();
        assertEquals("Expected 1 result!", 1, count);
    }

    @Test
    public void rangeFilterDecimalTest3() {
        int count = cassandraUtils.filter(range("decimal_1").lower("2000000000.0").upper("3000000000.0")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterDecimalTest4() {
        int count = cassandraUtils.filter(range("decimal_1").lower("2000000000.0")
                                                            .upper("3000000000.0")
                                                            .includeLower(true)
                                                            .includeUpper(true)).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeFilterDecimalTest5() {
        int count = cassandraUtils.filter(range("decimal_1").lower("2000000000.000001").upper("2000000000.181235"))
                                  .count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterDoubleTest1() {
        int count = cassandraUtils.filter(range("double_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeFilterDoubleTest2() {
        int count = cassandraUtils.filter(range("double_1").lower("1.9").upper("2.1")).count();
        assertEquals("Expected 1 result!", 1, count);
    }

    @Test
    public void rangeFilterDoubleTest3() {
        int count = cassandraUtils.filter(range("double_1").lower("2.0").upper("3.0")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterDoubleTest4() {
        int count = cassandraUtils.filter(range("double_1").lower("2.0")
                                                           .upper("3.0")
                                                           .includeLower(true)
                                                           .includeUpper(true)).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeFilterDoubleTest5() {
        int count = cassandraUtils.filter(range("double_1").lower("7.0")
                                                           .upper("10.0")
                                                           .includeLower(true)
                                                           .includeUpper(true)).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterFloatTest1() {
        int count = cassandraUtils.filter(range("float_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeFilterFloatTest2() {
        int count = cassandraUtils.filter(range("float_1").lower("1.9").upper("2.1")).count();
        assertEquals("Expected 1 result!", 1, count);
    }

    @Test
    public void rangeFilterFloatTest3() {
        int count = cassandraUtils.filter(range("float_1").lower("1.0").upper("2.0")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterFloatTest4() {
        int count = cassandraUtils.filter(range("float_1").lower("1.0")
                                                          .upper("2.0")
                                                          .includeLower(true)
                                                          .includeUpper(true)).count();
        assertEquals("Expected 2 results!", 2, count);
    }

    @Test
    public void rangeFilterFloatTest5() {
        int count = cassandraUtils.filter(range("float_1").lower("7.0").upper("9.0")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterUuidTest1() {
        int count = cassandraUtils.filter(range("uuid_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test(expected = InvalidQueryException.class)
    public void rangeFilterUuidTest2() {
        cassandraUtils.filter(range("uuid_1").lower("1").upper("9")).count();
    }

    @Test
    public void rangeFilterUuidTest3() {
        int count = cassandraUtils.filter(range("uuid_1").lower("60297440-b4fa-11e3-8b5a-0002a5d5c51c")
                                                         .upper("60297440-b4fa-11e3-8b5a-0002a5d5c51d")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterUuidTest4() {
        int count = cassandraUtils.filter(range("uuid_1").lower("60297440-b4fa-11e3-8b5a-0002a5d5c51c")
                                                         .upper("60297440-b4fa-11e3-8b5a-0002a5d5c51d")
                                                         .includeLower(true)
                                                         .includeUpper(true)).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeFilterTimeuuidTest1() {
        int count = cassandraUtils.filter(range("timeuuid_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test(expected = InvalidQueryException.class)
    public void rangeFilterTimeuuidTest2() {
        int count = cassandraUtils.filter(range("timeuuid_1").lower("a").upper("z")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeFilterTimeuuidTest3() {
        int count = cassandraUtils.filter(range("timeuuid_1").lower("a4a70900-24e1-11df-8924-001ff3591712")
                                                             .upper("a4a70900-24e1-11df-8924-001ff3591713")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterTimeuuidTest4() {
        int count = cassandraUtils.filter(range("timeuuid_1").lower("a4a70900-24e1-11df-8924-001ff3591712")
                                                             .upper("a4a70900-24e1-11df-8924-001ff3591713")
                                                             .includeLower(true)
                                                             .includeUpper(true)).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeFilterInetFieldTest1() {
        int count = cassandraUtils.filter(range("inet_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeFilterInetFieldTest2() {
        int count = cassandraUtils.filter(range("inet_1").lower("127.0.0.0").upper("127.1.0.0")).count();
        assertEquals("Expected 2 results!", 2, count);
    }

    @Test
    public void rangeFilterInetFieldTest3() {
        int count = cassandraUtils.filter(range("inet_1").lower("127.0.0.0")
                                                         .upper("127.1.0.0")
                                                         .includeLower(true)
                                                         .includeUpper(true)).count();
        assertEquals("Expected 2 results!", 2, count);
    }

    @Test
    public void rangeFilterInetFieldTest4() {
        int count = cassandraUtils.filter(range("inet_1").lower("192.168.0.0").upper("192.168.0.1")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterTextFieldTest1() {

        int count = cassandraUtils.filter(range("text_1")).count();

        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeFilterTextFieldTest2() {
        int count = cassandraUtils.filter(range("text_1").lower("frase").upper("g")).count();
        assertEquals("Expected 3 results!", 3, count);
    }

    @Test
    public void rangeFilterTextFieldTest3() {
        int count = cassandraUtils.filter(range("text_1").lower(
                "frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga").upper("g")).count();
        assertEquals("Expected 1 result!", 1, count);
    }

    @Test
    public void rangeFilterTextFieldTest4() {
        int count = cassandraUtils.filter(range("text_1").lower(
                "frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga")
                                                         .upper("g")
                                                         .includeLower(true)
                                                         .includeUpper(true)).count();
        assertEquals("Expected 2 results!", 2, count);
    }

    @Test
    public void rangeFilterTextFieldTest5() {
        int count = cassandraUtils.filter(range("text_1").lower("G").upper("H").includeLower(true).includeUpper(true))
                                  .count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterVarcharFieldTest1() {
        int count = cassandraUtils.filter(range("varchar_1")).count();
        assertEquals("Expected 5 results!", 5, count);
    }

    @Test
    public void rangeFilterVarcharFieldTest2() {
        int count = cassandraUtils.filter(range("varchar_1").lower("frase").upper("g")).count();
        assertEquals("Expected 4 results!", 4, count);
    }

    @Test
    public void rangeFilterVarcharFieldTest3() {
        int count = cassandraUtils.filter(range("varchar_1").lower("frasesencillasinespaciosperomaslarga").upper("g"))
                                  .count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterVarcharFieldTest4() {
        int count = cassandraUtils.filter(range("varchar_1").lower("frasesencillasinespaciosperomaslarga")
                                                            .upper("gH")
                                                            .includeLower(true)
                                                            .includeUpper(true)).count();
        assertEquals("Expected 2 results!", 2, count);
    }

    @Test
    public void rangeFilterVarcharFieldTest5() {
        int count = cassandraUtils.filter(range("varchar_1").lower("g").upper("h")).count();
        assertEquals("Expected 0 results!", 0, count);
    }

    @Test
    public void rangeFilterListFieldTest1() {
        int n = cassandraUtils.filter(range("list_1").lower("a").upper("z")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void rangeFilterListFieldTest2() {
        int n = cassandraUtils.filter(range("list_1").lower("a1").upper("z9")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void rangeFilterListFieldTest3() {
        int n = cassandraUtils.filter(range("list_1").lower("a2").upper("l1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void rangeFilterSetFieldTest1() {
        int n = cassandraUtils.filter(range("set_1").lower("a").upper("z")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void rangeFilterSetFieldTest2() {
        int n = cassandraUtils.filter(range("set_1").lower("a1").upper("z9")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void rangeFilterSetFieldTest3() {
        int n = cassandraUtils.filter(range("set_1").lower("a1").upper("z1")).count();
        assertEquals("Expected 5 results!", 5, n);
    }

    @Test
    public void rangeFilterMapFieldTest1() {
        int n = cassandraUtils.filter(range("map_1.k1").lower("a").upper("z")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void rangeFilterMapFieldTest2() {
        int n = cassandraUtils.filter(range("map_1.k1").lower("a1").upper("z9")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void rangeFilterMapFieldTest3() {
        int n = cassandraUtils.filter(range("map_1.k1").lower("a1").upper("k9")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void rangeFilterMapFieldTest4() {
        int n = cassandraUtils.filter(range("map_1.k1").lower("a1").upper("k1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }
}
