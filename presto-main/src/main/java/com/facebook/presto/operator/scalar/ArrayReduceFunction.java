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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.type.TypeUtils.castValue;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class ArrayReduceFunction
        extends ParametricScalar
{
    public static final ArrayReduceFunction ARRAY_REDUCE = new ArrayReduceFunction();
    private static final Signature SIGNATURE = new Signature("reduce", ImmutableList.of(typeParameter("T"), typeParameter("R")), "R", ImmutableList.of("array<T>", "function<T,R,R>", "R"), false, false);
    private static final MethodHandle LONG_METHOD_HANDLE = methodHandle(ArrayReduceFunction.class, "arrayReduce", Type.class, Block.class, MethodHandle.class, long.class);
    private static final MethodHandle DOUBLE_METHOD_HANDLE = methodHandle(ArrayReduceFunction.class, "arrayReduce", Type.class, Block.class, MethodHandle.class, double.class);
    private static final MethodHandle BOOLEAN_METHOD_HANDLE = methodHandle(ArrayReduceFunction.class, "arrayReduce", Type.class, Block.class, MethodHandle.class, boolean.class);
    private static final MethodHandle SLICE_METHOD_HANDLE = methodHandle(ArrayReduceFunction.class, "arrayReduce", Type.class, Block.class, MethodHandle.class, Slice.class);
    private static final MethodHandle BLOCK_METHOD_HANDLE = methodHandle(ArrayReduceFunction.class, "arrayReduce", Type.class, Block.class, MethodHandle.class, Block.class);

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
        return "Reduces an array to a single value, using the given lambda and the given initial value";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type inputType = types.get("T");
        Type outputType = types.get("R");
        TypeSignature lambdaSignature = parameterizedTypeName("function", inputType.getTypeSignature(), outputType.getTypeSignature(), outputType.getTypeSignature());
        Signature signature = new Signature("map", outputType.getTypeSignature(), parameterizedTypeName("array", inputType.getTypeSignature()), lambdaSignature, outputType.getTypeSignature());
        MethodHandle methodHandle;
        if (outputType.getJavaType() == long.class) {
            methodHandle = LONG_METHOD_HANDLE.bindTo(inputType);
        }
        else if (outputType.getJavaType() == double.class) {
            methodHandle = DOUBLE_METHOD_HANDLE.bindTo(inputType);
        }
        else if (outputType.getJavaType() == boolean.class) {
            methodHandle = BOOLEAN_METHOD_HANDLE.bindTo(inputType);
        }
        else if (outputType.getJavaType() == Slice.class) {
            methodHandle = SLICE_METHOD_HANDLE.bindTo(inputType);
        }
        else if (outputType.getJavaType() == Block.class) {
            methodHandle = BLOCK_METHOD_HANDLE.bindTo(inputType);
        }
        else {
            throw new UnsupportedOperationException("Unsupported type " + outputType.getJavaType());
        }

        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(false, false, false));
    }

    public static Block arrayReduce(Type inputType, Block block, MethodHandle lambda, Block initial)
    {
        Block result = initial;

        for (int i = 0; i < block.getPositionCount(); i++) {
            // TODO we should specialize on the element types to avoid having Object here
            Object value = castValue(inputType, block, i);
            if (value == null) {
                continue;
            }
            try {
                result = (Block) lambda.invoke(value, result);
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }
        }
        return result;
    }

    public static Slice arrayReduce(Type inputType, Block block, MethodHandle lambda, Slice initial)
    {
        Slice result = initial;

        for (int i = 0; i < block.getPositionCount(); i++) {
            // TODO we should specialize on the element types to avoid having Object here
            Object value = castValue(inputType, block, i);
            if (value == null) {
                continue;
            }
            try {
                result = (Slice) lambda.invoke(value, result);
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }
        }
        return result;
    }

    public static boolean arrayReduce(Type inputType, Block block, MethodHandle lambda, boolean initial)
    {
        boolean result = initial;

        for (int i = 0; i < block.getPositionCount(); i++) {
            // TODO we should specialize on the element types to avoid having Object here
            Object value = castValue(inputType, block, i);
            if (value == null) {
                continue;
            }
            try {
                result = (boolean) lambda.invoke(value, result);
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }
        }
        return result;
    }

    public static double arrayReduce(Type inputType, Block block, MethodHandle lambda, double initial)
    {
        double result = initial;

        for (int i = 0; i < block.getPositionCount(); i++) {
            // TODO we should specialize on the element types to avoid having Object here
            Object value = castValue(inputType, block, i);
            if (value == null) {
                continue;
            }
            try {
                result = (double) lambda.invoke(value, result);
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }
        }
        return result;
    }

    public static long arrayReduce(Type inputType, Block block, MethodHandle lambda, long initial)
    {
        long result = initial;

        for (int i = 0; i < block.getPositionCount(); i++) {
            // TODO we should specialize on the element types to avoid having Object here
            Object value = castValue(inputType, block, i);
            if (value == null) {
                continue;
            }
            try {
                result = (long) lambda.invoke(value, result);
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }
        }
        return result;
    }
}
