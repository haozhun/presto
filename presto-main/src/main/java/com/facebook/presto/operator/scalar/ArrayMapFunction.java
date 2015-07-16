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

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.type.TypeUtils.appendToBlockBuilder;
import static com.facebook.presto.type.TypeUtils.castValue;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class ArrayMapFunction
        extends ParametricScalar
{
    public static final ArrayMapFunction ARRAY_MAP = new ArrayMapFunction();
    private static final Signature SIGNATURE = new Signature("map", ImmutableList.of(typeParameter("T"), typeParameter("R")), "array<R>", ImmutableList.of("array<T>", "function<T,R>"), false, false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayMapFunction.class, "arrayMap", Type.class, Type.class, Block.class, MethodHandle.class);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "Returns an array which is the result of apply the given function to each element";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type inputType = types.get("T");
        Type outputType = types.get("R");
        Signature signature = new Signature("map", parameterizedTypeName("array", outputType.getTypeSignature()), parameterizedTypeName("array", inputType.getTypeSignature()), parameterizedTypeName("function", inputType.getTypeSignature(), outputType.getTypeSignature()));
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(inputType).bindTo(outputType);
        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(false, false));
    }

    public static Block arrayMap(Type inputType, Type outputType, Block block, MethodHandle lambda)
    {
        if (block.getPositionCount() == 0) {
            return outputType.createBlockBuilder(new BlockBuilderStatus(), 0).build();
        }
        BlockBuilder builder = outputType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount(), block.getSizeInBytes() / block.getPositionCount());

        for (int i = 0; i < block.getPositionCount(); i++) {
            // TODO we should specialize on the element types to avoid having Object here
            Object value = castValue(inputType, block, i);
            Object result;
            if (value == null) {
                result = null;
            }
            else {
                try {
                    result = lambda.invoke(value);
                }
                catch (Throwable throwable) {
                    throw Throwables.propagate(throwable);
                }
            }
            appendToBlockBuilder(outputType, result, builder);
        }
        return builder.build();
    }
}
