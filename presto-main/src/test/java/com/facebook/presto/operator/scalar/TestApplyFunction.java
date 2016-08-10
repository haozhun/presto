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

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;

public class TestApplyFunction
        extends AbstractTestFunctions
{
    @Test
    public void testApply()
            throws Exception
    {
        assertFunction("APPLY(x -> x + 1, 5)", INTEGER, 6);
        assertFunction("APPLY(x -> x + 1, 5 + RANDOM(1))", INTEGER, 6);
        assertFunction("TRY(APPLY(x -> x + 1, 5 + RANDOM(1)) / 0)", INTEGER, null);
        if (true) {
            return;
        }
        assertFunction("APPLY(x -> x + TRY(1 / 0), 5 + RANDOM(1))", INTEGER, 6);
        assertInvalidFunction("APPLY(x -> x + TRY(1 / 0), 5 + RANDOM(1))", NOT_SUPPORTED);
    }

    @Test
    public void testTypeCombinations()
            throws Exception
    {
        assertFunction("APPLY(x -> x + 1, 25)", INTEGER, 26);
        assertFunction("APPLY(x -> x + 1.0, 25)", DOUBLE, 26.0);
        assertFunction("APPLY(x -> x = 25, 25)", BOOLEAN, true);
        assertFunction("APPLY(x -> to_base(x, 16), 25)", createUnboundedVarcharType(), "19");
        assertFunction("APPLY(x -> ARRAY[x + 1], 25)", new ArrayType(INTEGER), ImmutableList.of(26));
    }
}
