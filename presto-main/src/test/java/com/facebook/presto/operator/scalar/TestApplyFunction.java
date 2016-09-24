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

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;

public class TestApplyFunction
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
            throws Exception
    {
        assertFunction("apply(x -> x + 1, 5)", INTEGER, 6);
        assertFunction("apply(x -> x + 1, 5 + RANDOM(1))", INTEGER, 6);
    }

    @Test
    public void testWithTry()
            throws Exception
    {
        assertFunction("TRY(apply(x -> x + 1, 5) / 0)", INTEGER, null);
        assertFunction("TRY(apply(x -> x + 1, 5 + RANDOM(1)) / 0)", INTEGER, null);
        assertInvalidFunction("apply(x -> x + TRY(1 / 0), 5 + RANDOM(1))", NOT_SUPPORTED);
    }

    @Test
    public void testNestedLambda()
            throws Exception
    {
        assertFunction("apply(x -> apply(y -> apply(z -> z * 5, y * 3) + 1, x + 7) * 2, 11)", INTEGER, 542);
        assertFunction("apply(x -> apply(x -> apply(x -> x * 5, x * 3) + 1, x + 7) * 2, 11)", INTEGER, 542);
    }

    @Test
    public void testTypeCombinations()
            throws Exception
    {
        assertFunction("apply(x -> x + 1, 25)", INTEGER, 26);
        assertFunction("apply(x -> x + 1.0, 25)", DOUBLE, 26.0);
        assertFunction("apply(x -> x = 25, 25)", BOOLEAN, true);
        assertFunction("apply(x -> to_base(x, 16), 25)", createUnboundedVarcharType(), "19");
        assertFunction("apply(x -> ARRAY[x + 1], 25)", new ArrayType(INTEGER), ImmutableList.of(26));

        assertFunction("apply(x -> CAST(x AS BIGINT), 25.6)", BIGINT, 26L);
        assertFunction("apply(x -> x + 1.0, 25.6)", DOUBLE, 26.6);
        assertFunction("apply(x -> x = 25.6, 25.6)", BOOLEAN, true);
        assertFunction("apply(x -> CAST(x AS VARCHAR), 25.6)", createUnboundedVarcharType(), "25.6");
        assertFunction("apply(x -> MAP(ARRAY[x + 1], ARRAY[true]), 25.6)", new MapType(DOUBLE, BOOLEAN), ImmutableMap.of(26.6, true));

        assertFunction("apply(x -> if(x, 25, 26), true)", INTEGER, 25);
        assertFunction("apply(x -> if(x, 25.6, 28.9), false)", DOUBLE, 28.9);
        assertFunction("apply(x -> not x, true)", BOOLEAN, false);
        assertFunction("apply(x -> CAST(x AS VARCHAR), false)", createUnboundedVarcharType(), "false");
        assertFunction("apply(x -> ARRAY[x], true)", new ArrayType(BOOLEAN), ImmutableList.of(true));

        assertFunction("apply(x -> from_base(x, 16), '41')", BIGINT, 65L);
        assertFunction("apply(x -> CAST(x AS DOUBLE), '25.6')", DOUBLE, 25.6);
        assertFunction("apply(x -> 'abc' = x, 'abc')", BOOLEAN, true);
        assertFunction("apply(x -> x || x, 'abc')", createUnboundedVarcharType(), "abcabc");
        assertFunction(
                "apply(x -> ROW(x, CAST(x AS INTEGER), x > '0'), '123')",
                new RowType(ImmutableList.of(createVarcharType(3), INTEGER, BOOLEAN), Optional.empty()),
                ImmutableList.of("123", 123, true));

        assertFunction("apply(x -> from_base(x[3], 10), ARRAY['abc', null, '123'])", BIGINT, 123L);
        assertFunction("apply(x -> CAST(x[3] AS DOUBLE), ARRAY['abc', null, '123'])", DOUBLE, 123.0);
        assertFunction("apply(x -> x[2] IS NULL, ARRAY['abc', null, '123'])", BOOLEAN, true);
        assertFunction("apply(x -> x[2], ARRAY['abc', null, '123'])", createVarcharType(3), null);
        assertFunction("apply(x -> map_keys(x), MAP(ARRAY['abc', 'def'], ARRAY[123, 456]))", new ArrayType(createVarcharType(3)), ImmutableList.of("abc", "def"));
    }
}
