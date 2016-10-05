/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.operator.scalar;

import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.util.Arrays.asList;

public class TestArrayFilterFunction
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
            throws Exception
    {
        assertFunction("filter(x -> x = 5, ARRAY [5, 6])", new ArrayType(INTEGER), ImmutableList.of(5));
        assertFunction("filter(x -> x = 5, ARRAY [5 + RANDOM(1), 6 + RANDOM(1)])", new ArrayType(INTEGER), ImmutableList.of(5));
        assertFunction("filter(x -> nullif(x, false), ARRAY [true, false, true, false])", new ArrayType(BOOLEAN), ImmutableList.of(true, true));
        assertFunction("filter(x -> not x, ARRAY [true, false, null, true, false, null])", new ArrayType(BOOLEAN), ImmutableList.of(false, false));
    }

    @Test
    public void testTypeCombinations()
            throws Exception
    {
        assertFunction("filter(x -> x % 2 = 1, ARRAY [25, 26, 27])", new ArrayType(INTEGER), ImmutableList.of(25, 27));
        assertFunction("filter(x -> x < 30.0, ARRAY [25.6, 37.3, 28.6])", new ArrayType(DOUBLE), ImmutableList.of(25.6, 28.6));
        assertFunction("filter(x -> not x, ARRAY [true, false, true])", new ArrayType(BOOLEAN), ImmutableList.of(false));
        assertFunction("filter(x -> substr(x, 1, 1) = 'a', ARRAY ['abc', 'def', 'ayz'])", new ArrayType(createVarcharType(3)), ImmutableList.of("abc", "ayz"));
        assertFunction(
                "filter(x -> x[2] IS NULL, ARRAY [ARRAY ['abc', null, '123'], ARRAY ['def', 'x', '456']])",
                new ArrayType(new ArrayType(createVarcharType(3))),
                ImmutableList.of(asList("abc", null, "123")));
    }
}
