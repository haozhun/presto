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
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.util.Arrays.asList;

public class TestArrayTransformFunction
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
            throws Exception
    {
        assertFunction("transform(x -> x + 1, ARRAY [5, 6])", new ArrayType(INTEGER), ImmutableList.of(6, 7));
        assertFunction("transform(x -> x + 1, ARRAY [5 + RANDOM(1), 6])", new ArrayType(INTEGER), ImmutableList.of(6, 7));
    }

    @Test
    public void testTypeCombinations()
            throws Exception
    {
        assertFunction("transform(x -> x + 1, ARRAY [25, 26])", new ArrayType(INTEGER), ImmutableList.of(26, 27));
        assertFunction("transform(x -> x + 1.0, ARRAY [25, 26])", new ArrayType(DOUBLE), ImmutableList.of(26.0, 27.0));
        assertFunction("transform(x -> x = 25, ARRAY [25, 26])", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        assertFunction("transform(x -> to_base(x, 16), ARRAY [25, 26])", new ArrayType(createUnboundedVarcharType()), ImmutableList.of("19", "1a"));
        assertFunction("transform(x -> ARRAY[x + 1], ARRAY [25, 26])", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(26), ImmutableList.of(27)));

        assertFunction("transform(x -> CAST(x AS BIGINT), ARRAY [25.6, 27.3])", new ArrayType(BIGINT), ImmutableList.of(26L, 27L));
        assertFunction("transform(x -> x + 1.0, ARRAY [25.6, 27.3])", new ArrayType(DOUBLE), ImmutableList.of(26.6, 28.3));
        assertFunction("transform(x -> x = 25.6, ARRAY [25.6, 27.3])", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        assertFunction("transform(x -> CAST(x AS VARCHAR), ARRAY [25.6, 27.3])", new ArrayType(createUnboundedVarcharType()), ImmutableList.of("25.6", "27.3"));
        assertFunction(
                "transform(x -> MAP(ARRAY[x + 1], ARRAY[true]), ARRAY [25.6, 27.3])",
                new ArrayType(new MapType(DOUBLE, BOOLEAN)),
                ImmutableList.of(ImmutableMap.of(26.6, true), ImmutableMap.of(28.3, true)));

        assertFunction("transform(x -> if(x, 25, 26), ARRAY [true, false])", new ArrayType(INTEGER), ImmutableList.of(25, 26));
        assertFunction("transform(x -> if(x, 25.6, 28.9), ARRAY [false, true])", new ArrayType(DOUBLE), ImmutableList.of(28.9, 25.6));
        assertFunction("transform(x -> not x, ARRAY [true, false])", new ArrayType(BOOLEAN), ImmutableList.of(false, true));
        assertFunction("transform(x -> CAST(x AS VARCHAR), ARRAY [false, true])", new ArrayType(createUnboundedVarcharType()), ImmutableList.of("false", "true"));
        assertFunction("transform(x -> ARRAY[x], ARRAY [true, false])", new ArrayType(new ArrayType(BOOLEAN)), ImmutableList.of(ImmutableList.of(true), ImmutableList.of(false)));

        assertFunction("transform(x -> from_base(x, 16), ARRAY ['41', '42'])", new ArrayType(BIGINT), ImmutableList.of(65L, 66L));
        assertFunction("transform(x -> CAST(x AS DOUBLE), ARRAY ['25.6', '27.3'])", new ArrayType(DOUBLE), ImmutableList.of(25.6, 27.3));
        assertFunction("transform(x -> 'abc' = x, ARRAY ['abc', 'def'])", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        assertFunction("transform(x -> x || x, ARRAY ['abc', 'def'])", new ArrayType(createUnboundedVarcharType()), ImmutableList.of("abcabc", "defdef"));
        assertFunction(
                "transform(x -> ROW(x, CAST(x AS INTEGER), x > '3'), ARRAY ['123', '456'])",
                new ArrayType(new RowType(ImmutableList.of(createVarcharType(3), INTEGER, BOOLEAN), Optional.empty())),
                ImmutableList.of(ImmutableList.of("123", 123, false), ImmutableList.of("456", 456, true)));

        assertFunction(
                "transform(x -> from_base(x[3], 10), ARRAY [ARRAY ['abc', null, '123'], ARRAY ['def', 'x', '456']])",
                new ArrayType(BIGINT),
                ImmutableList.of(123L, 456L));
        assertFunction(
                "transform(x -> CAST(x[3] AS DOUBLE), ARRAY [ARRAY ['abc', null, '123'], ARRAY ['def', 'x', '456']])",
                new ArrayType(DOUBLE),
                ImmutableList.of(123.0, 456.0));
        assertFunction(
                "transform(x -> x[2] IS NULL, ARRAY [ARRAY ['abc', null, '123'], ARRAY ['def', 'x', '456']])",
                new ArrayType(BOOLEAN),
                ImmutableList.of(true, false));
        assertFunction(
                "transform(x -> x[2], ARRAY [ARRAY ['abc', null, '123'], ARRAY ['def', 'x', '456']])",
                new ArrayType(createVarcharType(3)),
                asList(null, "x"));
        assertFunction(
                "transform(x -> map_keys(x), ARRAY [MAP(ARRAY['abc', 'def'], ARRAY[123, 456]), MAP(ARRAY['ghi', 'jkl'], ARRAY[234, 567])])",
                new ArrayType(new ArrayType(createVarcharType(3))),
                ImmutableList.of(ImmutableList.of("abc", "def"), ImmutableList.of("ghi", "jkl")));
    }
}
