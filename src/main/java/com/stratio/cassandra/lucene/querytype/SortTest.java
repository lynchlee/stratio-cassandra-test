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

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.sortField;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class SortTest extends AbstractWatchedTest {

    @Test
    public void sortIntegerAsc() {
        Integer[] returnedValues = cassandraUtils.sort(sortField("integer_1").reverse(false)).intColumn("integer_1");
        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortIntegerDesc() {
        Integer[] returnedValues = cassandraUtils.sort(sortField("integer_1").reverse(true)).intColumn("integer_1");
        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-1, -2, -3, -4, -5};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortIntegerDefault() {
        Integer[] returnedValues = cassandraUtils.sort(sortField("integer_1")).intColumn("integer_1");
        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Integer[] expectedValues = new Integer[]{-5, -4, -3, -2, -1};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortDoubleAsc() {
        Double[] returnedValues = cassandraUtils.sort(sortField("double_1").reverse(false)).doubleColumn("double_1");
        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Double[] expectedValues = new Double[]{1D, 2D, 3D, 3D, 3D};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortDoubleDesc() {
        Double[] returnedValues = cassandraUtils.sort(sortField("double_1").reverse(true)).doubleColumn("double_1");
        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Double[] expectedValues = new Double[]{3D, 3D, 3D, 2D, 1D};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortDoubleDefault() {
        Double[] returnedValues = cassandraUtils.sort(sortField("double_1")).doubleColumn("double_1");
        assertEquals("Expected 5 results!", 5, returnedValues.length);
        Double[] expectedValues = new Double[]{1D, 2D, 3D, 3D, 3D};
        assertArrayEquals("Wrong sort!", expectedValues, returnedValues);
    }

    @Test
    public void sortCombined() {
        Double[] returnedDoubleValues = cassandraUtils.sort(sortField("double_1"), sortField("integer_1"))
                                                      .doubleColumn("double_1");
        assertEquals("Expected 5 results!", 5, returnedDoubleValues.length);
        Integer[] returnedIntValues = cassandraUtils.sort(sortField("double_1"), sortField("integer_1"))
                                                    .intColumn("integer_1");
        assertEquals("Expected 5 results!", 5, returnedIntValues.length);
        Double[] expectedDoubleValues = new Double[]{1D, 2D, 3D, 3D, 3D};
        Integer[] expectedIntValues = new Integer[]{-1, -2, -5, -4, -3};
        assertArrayEquals("Wrong doubles sort!", expectedDoubleValues, returnedDoubleValues);
        assertArrayEquals("Wrong integers sort!", expectedIntValues, returnedIntValues);
    }
}
