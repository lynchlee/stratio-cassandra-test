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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static com.stratio.cassandra.index.query.builder.SearchBuilders.match;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class MatchTest extends AbstractWatchedTest {

    @Test
    public void matchQueryAsciiFieldTest1() {
        int n = cassandraUtils.query(match("ascii_1", "frase tipo ascii")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchQueryAsciiFieldTest2() {
        int n = cassandraUtils.query(match("ascii_1", "frase")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryAsciiFieldTest3() {
        int n = cassandraUtils.query(match("ascii_1", "frase tipo asciii")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test(expected = DriverInternalError.class)
    public void matchQueryAsciiFieldTest4() {
        int n = cassandraUtils.query(match("ascii_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryAsciiFieldTest5() {
        int n = cassandraUtils.query(match("ascii_1", "frase tipo asci")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryBigintTest2() {
        int n = cassandraUtils.query(match("bigint_1", "1000000000000000")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchQueryBigintTest3() {
        int n = cassandraUtils.query(match("bigint_1", "3000000000000000")).count();
        assertEquals("Expected 3 results!", 3, n);
    }

    @Test
    public void matchQueryBigintTest4() {
        int n = cassandraUtils.query(match("bigint_1", "10000000000000000")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryBigintTest5() {
        int n = cassandraUtils.query(match("bigint_1", "100000000000000")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test(expected = DriverInternalError.class)
    public void matchQueryBlobTest1() {
        int n = cassandraUtils.query(match("blob_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryBlobTest2() {
        int n = cassandraUtils.query(match("blob_1", "3E0A16")).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void matchQueryBlobTest3() {
        int n = cassandraUtils.query(match("blob_1", "3E0A161")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryBlobTest4() {
        int n = cassandraUtils.query(match("blob_1", "3E0A1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryBlobTest5() {
        int n = cassandraUtils.query(match("blob_1", "3E0A15")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test(expected = DriverInternalError.class)
    public void matchQueryBooleanTest1() {
        int n = cassandraUtils.query(match("boolean_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryBooleanTest3() {
        int n = cassandraUtils.query(match("boolean_1", "true")).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void matchQueryBooleanTest4() {
        int n = cassandraUtils.query(match("boolean_1", "false")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchQueryBooleanTest5() {
        int n = cassandraUtils.query(match("boolean_1", "else")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryDecimalTest2() {
        int n = cassandraUtils.query(match("decimal_1", "3000000000.0")).count();
        assertEquals("Expected 3 results!", 3, n);
    }

    @Test
    public void matchQueryDecimalTest3() {
        int n = cassandraUtils.query(match("decimal_1", "300000000.0")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryDecimalTest4() {
        int n = cassandraUtils.query(match("decimal_1", "3000000000.0")).count();
        assertEquals("Expected 3 results!", 3, n);
    }

    @Test
    public void matchQueryDecimalTest5() {
        int n = cassandraUtils.query(match("decimal_1", "1000000000.0")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchQueryDateTest1() {

        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Date date = new Date();

        int n = cassandraUtils.query(match("date_1", df.format(date))).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryDateTest2() {

        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);
        Date date = calendar.getTime();

        int n = cassandraUtils.query(match("date_1", df.format(date))).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryDateTest3() {
        int n = cassandraUtils.query(match("date_1", "1970/01/01 00:00:00.000")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryDateTest4() {

        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);
        Date date = calendar.getTime();

        int n = cassandraUtils.query(match("date_1", df.format(date))).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryDoubleTest1() {
        int n = cassandraUtils.query(match("double_1", "0")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryDoubleTest2() {
        int n = cassandraUtils.query(match("double_1", "2.0")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchQueryDoubleTest3() {
        int n = cassandraUtils.query(match("double_1", "2")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchQueryDoubleTest4() {
        int n = cassandraUtils.query(match("double_1", "2.00")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchQueryFloatTest1() {
        int n = cassandraUtils.query(match("float_1", "0")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryFloatTest2() {
        int n = cassandraUtils.query(match("float_1", "2.0")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchQueryFloatTest3() {
        int n = cassandraUtils.query(match("float_1", "2")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchQueryFloatTest4() {
        int n = cassandraUtils.query(match("float_1", "2.00")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchQueryIntegerTest1() {
        int n = cassandraUtils.query(match("integer_1", "-2")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchQueryIntegerTest2() {
        int n = cassandraUtils.query(match("integer_1", "2")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryIntegerTest3() {
        int n = cassandraUtils.query(match("integer_1", "0")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryIntegerTest4() {
        int n = cassandraUtils.query(match("integer_1", "-1")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchQueryUuidTest1() {
        int n = cassandraUtils.query(match("uuid_1", "0")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryUuidTest2() {

        int n = cassandraUtils.query(match("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51b")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchQueryUuidTest3() {
        int n = cassandraUtils.query(match("uuid_1", "60297440-b4fa-11e3-0002a5d5c51b")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryTimeuuidTest1() {
        int n = cassandraUtils.query(match("timeuuid_1", "0")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryTimeuuidTest2() {
        int n = cassandraUtils.query(match("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591711")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchQueryTimeuuidTest3() {
        int n = cassandraUtils.query(match("timeuuid_1", "a4a70900-24e1-11df-001ff3591711")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryInetFieldTest1() {
        int n = cassandraUtils.query(match("inet_1", "127.1.1.1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void matchQueryInetFieldTest2() {
        int n = cassandraUtils.query(match("inet_1", "127.0.1.1")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchQueryInetFieldTest3() {
        int n = cassandraUtils.query(match("inet_1", "127.1.1.")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test(expected = DriverInternalError.class)
    public void matchQueryInetFieldTest4() {
        int n = cassandraUtils.query(match("inet_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryTextFieldTest1() {
        int n = cassandraUtils.query(match("text_1", "Frase")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchQueryTextFieldTest2() {
        int n = cassandraUtils.query(match("text_1", "Frase*")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchQueryTextFieldTest3() {
        int n = cassandraUtils.query(match("text_1", "Frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga"))
                              .count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test(expected = DriverInternalError.class)
    public void matchQueryTextFieldTest4() {
        int n = cassandraUtils.query(match("text_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryVarcharFieldTest1() {
        int n = cassandraUtils.query(match("varchar_1", "frasesencillasinespaciosperomaslarga")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void matchQueryVarcharFieldTest2() {
        int n = cassandraUtils.query(match("varchar_1", "frase*")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryVarcharFieldTest3() {
        int n = cassandraUtils.query(match("varchar_1", "frasesencillasinespacios")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test(expected = DriverInternalError.class)
    public void matchQueryVarcharFieldTest4() {
        int n = cassandraUtils.query(match("varchar_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test(expected = DriverInternalError.class)
    public void matchQueryListFieldTest1() {
        int n = cassandraUtils.query(match("list_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryListFieldTest2() {
        int n = cassandraUtils.query(match("list_1", "l1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void matchQueryListFieldTest3() {
        int n = cassandraUtils.query(match("list_1", "s1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test(expected = DriverInternalError.class)
    public void matchQuerySetFieldTest1() {
        int n = cassandraUtils.query(match("set_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQuerySetFieldTest2() {
        int n = cassandraUtils.query(match("set_1", "l1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQuerySetFieldTest3() {
        int n = cassandraUtils.query(match("set_1", "s1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test(expected = DriverInternalError.class)
    public void matchQueryMapFieldTest1() {
        int n = cassandraUtils.query(match("map_1.k1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryMapFieldTest2() {
        int n = cassandraUtils.query(match("map_1.k1", "l1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryMapFieldTest3() {
        int n = cassandraUtils.query(match("map_1.k1", "k1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchQueryMapFieldTest4() {
        int n = cassandraUtils.query(match("map_1.k1", "v1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void matchFilterAsciiFieldTest1() {
        int n = cassandraUtils.filter(match("ascii_1", "frase tipo ascii")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchFilterAsciiFieldTest2() {
        int n = cassandraUtils.filter(match("ascii_1", "frase")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterAsciiFieldTest3() {
        int n = cassandraUtils.filter(match("ascii_1", "frase tipo asciii")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test(expected = DriverInternalError.class)
    public void matchFilterAsciiFieldTest4() {
        int n = cassandraUtils.filter(match("ascii_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterAsciiFieldTest5() {
        int n = cassandraUtils.filter(match("ascii_1", "frase tipo asci")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterBigintTest2() {
        int n = cassandraUtils.filter(match("bigint_1", "1000000000000000")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchFilterBigintTest3() {
        int n = cassandraUtils.filter(match("bigint_1", "3000000000000000")).count();
        assertEquals("Expected 3 results!", 3, n);
    }

    @Test
    public void matchFilterBigintTest4() {
        int n = cassandraUtils.filter(match("bigint_1", "10000000000000000")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterBigintTest5() {
        int n = cassandraUtils.filter(match("bigint_1", "100000000000000")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test(expected = DriverInternalError.class)
    public void matchFilterBlobTest1() {
        int n = cassandraUtils.filter(match("blob_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterBlobTest2() {
        int n = cassandraUtils.filter(match("blob_1", "3E0A16")).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void matchFilterBlobTest3() {
        int n = cassandraUtils.filter(match("blob_1", "3E0A161")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterBlobTest4() {
        int n = cassandraUtils.filter(match("blob_1", "3E0A1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterBlobTest5() {
        int n = cassandraUtils.filter(match("blob_1", "3E0A15")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test(expected = DriverInternalError.class)
    public void matchFilterBooleanTest1() {
        int n = cassandraUtils.filter(match("boolean_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterBooleanTest3() {
        int n = cassandraUtils.filter(match("boolean_1", "true")).count();
        assertEquals("Expected 4 results!", 4, n);
    }

    @Test
    public void matchFilterBooleanTest4() {
        int n = cassandraUtils.filter(match("boolean_1", "false")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchFilterBooleanTest5() {
        int n = cassandraUtils.filter(match("boolean_1", "else")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterDecimalTest2() {
        int n = cassandraUtils.filter(match("decimal_1", "3000000000.0")).count();
        assertEquals("Expected 3 results!", 3, n);
    }

    @Test
    public void matchFilterDecimalTest3() {
        int n = cassandraUtils.filter(match("decimal_1", "300000000.0")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterDecimalTest4() {
        int n = cassandraUtils.filter(match("decimal_1", "3000000000.0")).count();
        assertEquals("Expected 3 results!", 3, n);
    }

    @Test
    public void matchFilterDecimalTest5() {
        int n = cassandraUtils.filter(match("decimal_1", "1000000000.0")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchFilterDateTest1() {

        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Date date = new Date();

        int n = cassandraUtils.filter(match("date_1", df.format(date))).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterDateTest2() {

        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);
        Date date = calendar.getTime();

        int n = cassandraUtils.filter(match("date_1", df.format(date))).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterDateTest3() {
        int n = cassandraUtils.filter(match("date_1", "1970/01/01 00:00:00.000")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterDateTest4() {

        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);
        Date date = calendar.getTime();

        int n = cassandraUtils.filter(match("date_1", df.format(date))).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterDoubleTest1() {
        int n = cassandraUtils.filter(match("double_1", "0")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterDoubleTest2() {
        int n = cassandraUtils.filter(match("double_1", "2.0")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchFilterDoubleTest3() {
        int n = cassandraUtils.filter(match("double_1", "2")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchFilterDoubleTest4() {
        int n = cassandraUtils.filter(match("double_1", "2.00")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchFilterFloatTest1() {
        int n = cassandraUtils.filter(match("float_1", "0")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterFloatTest2() {
        int n = cassandraUtils.filter(match("float_1", "2.0")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchFilterFloatTest3() {
        int n = cassandraUtils.filter(match("float_1", "2")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchFilterFloatTest4() {
        int n = cassandraUtils.filter(match("float_1", "2.00")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchFilterIntegerTest1() {
        int n = cassandraUtils.filter(match("integer_1", "-2")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchFilterIntegerTest2() {
        int n = cassandraUtils.filter(match("integer_1", "2")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterIntegerTest3() {
        int n = cassandraUtils.filter(match("integer_1", "0")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterIntegerTest4() {
        int n = cassandraUtils.filter(match("integer_1", "-1")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchFilterUuidTest1() {
        int n = cassandraUtils.filter(match("uuid_1", "0")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterUuidTest2() {

        int n = cassandraUtils.filter(match("uuid_1", "60297440-b4fa-11e3-8b5a-0002a5d5c51b")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchFilterUuidTest3() {
        int n = cassandraUtils.filter(match("uuid_1", "60297440-b4fa-11e3-0002a5d5c51b")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterTimeuuidTest1() {
        int n = cassandraUtils.filter(match("timeuuid_1", "0")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterTimeuuidTest2() {
        int n = cassandraUtils.filter(match("timeuuid_1", "a4a70900-24e1-11df-8924-001ff3591711")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchFilterTimeuuidTest3() {
        int n = cassandraUtils.filter(match("timeuuid_1", "a4a70900-24e1-11df-001ff3591711")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterInetFieldTest1() {
        int n = cassandraUtils.filter(match("inet_1", "127.1.1.1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void matchFilterInetFieldTest2() {
        int n = cassandraUtils.filter(match("inet_1", "127.0.1.1")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchFilterInetFieldTest3() {
        int n = cassandraUtils.filter(match("inet_1", "127.1.1.")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test(expected = DriverInternalError.class)
    public void matchFilterInetFieldTest4() {
        int n = cassandraUtils.filter(match("inet_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterTextFieldTest1() {
        int n = cassandraUtils.filter(match("text_1", "Frase")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchFilterTextFieldTest2() {
        int n = cassandraUtils.filter(match("text_1", "Frase*")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test
    public void matchFilterTextFieldTest3() {
        int n = cassandraUtils.filter(match("text_1",
                                            "Frasesinespaciosconarticulosylaspalabrassuficientesperomaslarga")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test(expected = DriverInternalError.class)
    public void matchFilterTextFieldTest4() {
        int n = cassandraUtils.filter(match("text_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterVarcharFieldTest1() {
        int n = cassandraUtils.filter(match("varchar_1", "frasesencillasinespaciosperomaslarga")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void matchFilterVarcharFieldTest2() {
        int n = cassandraUtils.filter(match("varchar_1", "frase*")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterVarcharFieldTest3() {
        int n = cassandraUtils.filter(match("varchar_1", "frasesencillasinespacios")).count();
        assertEquals("Expected 1 result!", 1, n);
    }

    @Test(expected = DriverInternalError.class)
    public void matchFilterVarcharFieldTest4() {
        int n = cassandraUtils.filter(match("varchar_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test(expected = DriverInternalError.class)
    public void matchFilterListFieldTest1() {
        int n = cassandraUtils.filter(match("list_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterListFieldTest2() {
        int n = cassandraUtils.filter(match("list_1", "l1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test
    public void matchFilterListFieldTest3() {
        int n = cassandraUtils.filter(match("list_1", "s1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test(expected = DriverInternalError.class)
    public void matchFilterSetFieldTest1() {
        int n = cassandraUtils.filter(match("set_1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterSetFieldTest2() {
        int n = cassandraUtils.filter(match("set_1", "l1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterSetFieldTest3() {
        int n = cassandraUtils.filter(match("set_1", "s1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }

    @Test(expected = DriverInternalError.class)
    public void matchFilterMapFieldTest1() {
        int n = cassandraUtils.filter(match("map_1.k1", "")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterMapFieldTest2() {
        int n = cassandraUtils.filter(match("map_1.k1", "l1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterMapFieldTest3() {
        int n = cassandraUtils.filter(match("map_1.k1", "k1")).count();
        assertEquals("Expected 0 results!", 0, n);
    }

    @Test
    public void matchFilterMapFieldTest4() {
        int n = cassandraUtils.filter(match("map_1.k1", "v1")).count();
        assertEquals("Expected 2 results!", 2, n);
    }
}
