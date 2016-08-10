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

package com.facebook.presto.sql.planner;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.util.ImmutableCollectors;
import com.facebook.presto.util.Reflection;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.CompilerUtils.makeClassName;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.sql.gen.BytecodeUtils.loadConstant;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LambdaExpressionStubGenerator
{
    private LambdaExpressionStubGenerator()
    {
    }

    public static MethodHandle generate(Type returnType, List<Type> argumentTypes, List<String> argumentNames, Function<SymbolResolver, Object> interpreter)
    {
        Preconditions.checkArgument(argumentNames.size() == argumentTypes.size());
        ImmutableList<Class<?>> javaTypes = argumentTypes.stream()
                .map(Type::getJavaType)
                .collect(ImmutableCollectors.toImmutableList());

        /*
        if (!ImmutableList.of(long.class).equals(javaTypes)) {
            throw new UnsupportedOperationException();
        }

        Class<?>[] methodTypes = new Class<?>[javaTypes.size() + 2];
        methodTypes[0] = Function.class;
        methodTypes[1] = List.class;
        for (int i = 2; i < methodTypes.length; i++) {
            methodTypes[i] = javaTypes.get(i - 2);
        }
        Function<Map<String, Object>, Object> function = map -> interpreter.apply(new LambdaSymbolResolver(map));
        MethodHandle methodHandle = Reflection.methodHandle(LambdaExpressionStubGenerator.class, "varArgsToMap", methodTypes)
                .bindTo(function)
                .bindTo(argumentNames);
        methodHandle = methodHandle.asType(methodHandle.type().changeReturnType(returnType.getJavaType()));
        /*/
        Function<Map<String, Object>, Object> function = map -> interpreter.apply(new LambdaSymbolResolver(map));
        Class<?> aClass = f(returnType.getJavaType(), javaTypes, argumentNames, function);
        MethodHandle methodHandle = Reflection.methodHandle(aClass, "varArgsToMap", javaTypes.toArray(new Class<?>[javaTypes.size()]));
        //*/

        return methodHandle;
    }

    private static Class<?> f(Class<?> returnType, List<Class<?>> javaTypes, List<String> names, Function<Map<String, Object>, Object> function) {

        CallSiteBinder callSiteBinder = new CallSiteBinder();

        ClassDefinition classDefinition = new ClassDefinition(a(PUBLIC, FINAL), makeClassName("LambdaStub"), type(Object.class));

        ImmutableList.Builder<Parameter> parameterListBuilder = ImmutableList.builder();
        //Parameter functionArgument = arg("function", Function.class);
        for (int i = 0; i < javaTypes.size(); i++) {
            Class<?> javaType = javaTypes.get(i);
            parameterListBuilder.add(arg("input_" + i, javaType));
        }
        ImmutableList<Parameter> parameterList = parameterListBuilder.build();

        MethodDefinition methodDefinition = classDefinition.declareMethod(a(PUBLIC, STATIC), "varArgsToMap", ParameterizedType.type(returnType), parameterList);
        BytecodeBlock body = methodDefinition.getBody();

        /*
        Variable mapBuilder = methodDefinition.getScope().declareVariable(ImmutableMap.Builder.class, "mapBuilder");

        body.append(mapBuilder.set(invokeStatic(ImmutableList.class, "builder", ImmutableMap.Builder.class)));
        for (int i = 0; i < javaTypes.size(); i++) {
            body.append(mapBuilder.invoke("put", ImmutableMap.Builder.class, constantString(names.get(i)), parameterList.get(i)));
        }
        */
        BytecodeExpression mapBuilder = invokeStatic(ImmutableMap.class, "builder", ImmutableMap.Builder.class);
        for (int i = 0; i < javaTypes.size(); i++) {
            mapBuilder = mapBuilder.invoke("put", ImmutableMap.Builder.class, constantString(names.get(i)).cast(Object.class), parameterList.get(i).cast(Object.class));
        }
        body.append(
                loadConstant(callSiteBinder, function, Function.class)
                        .invoke("apply", Object.class, mapBuilder.invoke("build", ImmutableMap.class).cast(Object.class))
                        .cast(returnType)
                        .ret());

        return defineClass(classDefinition, Object.class, callSiteBinder.getBindings(), new DynamicClassLoader(LambdaExpressionStubGenerator.class.getClassLoader()));
    }

    public static Object varArgsToMap(Function<Map<String, Object>, Object> function, List<String> names, long x)
    {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put(names.get(0), x);
        return function.apply(builder.build());
    }

    private static class LambdaSymbolResolver
            implements SymbolResolver
    {
        private final Map<String, Object> values;

        public LambdaSymbolResolver(Map<String, Object> values)
        {
            this.values = requireNonNull(values, "values is null");
        }

        @Override
        public Object getValue(Symbol symbol)
        {
            checkState(values.containsKey(symbol.getName()), "values does not contain %s", symbol);
            return values.get(symbol.getName());
        }
    }
}
