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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.util.ImmutableCollectors;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkNotNull;

public final class Signature
{
    private final String name;
    private final List<TypeParameter> typeParameters;
    private final TypeSignature returnType;
    private final List<TypeSignature> argumentTypes;
    private final boolean variableArity;
    private final boolean internal;

    @JsonCreator
    public Signature(
            @JsonProperty("name") String name,
            @JsonProperty("typeParameters") List<TypeParameter> typeParameters,
            @JsonProperty("returnType") TypeSignature returnType,
            @JsonProperty("argumentTypes") List<TypeSignature> argumentTypes,
            @JsonProperty("variableArity") boolean variableArity,
            @JsonProperty("internal") boolean internal)
    {
        checkNotNull(name, "name is null");
        checkNotNull(typeParameters, "typeParameters is null");

        this.name = name;
        this.typeParameters = ImmutableList.copyOf(typeParameters);
        this.returnType = checkNotNull(returnType, "returnType is null");
        this.argumentTypes = ImmutableList.copyOf(checkNotNull(argumentTypes, "argumentTypes is null"));
        this.variableArity = variableArity;
        this.internal = internal;
    }

    public Signature(String name, List<TypeParameter> typeParameters, String returnType, List<String> argumentTypes, boolean variableArity, boolean internal)
    {
        this(name, typeParameters, parseTypeSignature(returnType), Lists.transform(argumentTypes, TypeSignature::parseTypeSignature), variableArity, internal);
    }

    public Signature(String name, String returnType, List<String> argumentTypes)
    {
        this(name, ImmutableList.<TypeParameter>of(), parseTypeSignature(returnType), Lists.transform(argumentTypes, TypeSignature::parseTypeSignature), false, false);
    }

    public Signature(String name, String returnType, String... argumentTypes)
    {
        this(name, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public Signature(String name, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        this(name, ImmutableList.<TypeParameter>of(), returnType, argumentTypes, false, false);
    }

    public Signature(String name, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        this(name, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalOperator(String name, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return internalFunction(mangleOperatorName(name), returnType, argumentTypes);
    }

    public static Signature internalOperator(String name, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalFunction(mangleOperatorName(name), returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalFunction(String name, String returnType, String... argumentTypes)
    {
        return internalFunction(name, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalFunction(String name, String returnType, List<String> argumentTypes)
    {
        return new Signature(name, ImmutableList.<TypeParameter>of(), returnType, argumentTypes, false, true);
    }

    public static Signature internalFunction(String name, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalFunction(name, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalFunction(String name, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return new Signature(name, ImmutableList.<TypeParameter>of(), returnType, argumentTypes, false, true);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public TypeSignature getReturnType()
    {
        return returnType;
    }

    @JsonProperty
    public List<TypeSignature> getArgumentTypes()
    {
        return argumentTypes;
    }

    @JsonProperty
    public boolean isInternal()
    {
        return internal;
    }

    @JsonProperty
    public boolean isVariableArity()
    {
        return variableArity;
    }

    @JsonProperty
    public List<TypeParameter> getTypeParameters()
    {
        return typeParameters;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, typeParameters, returnType, argumentTypes, variableArity, internal);
    }

    Signature withAlias(String name)
    {
        return new Signature(name, typeParameters, getReturnType(), getArgumentTypes(), variableArity, internal);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Signature other = (Signature) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.typeParameters, other.typeParameters) &&
                Objects.equals(this.returnType, other.returnType) &&
                Objects.equals(this.argumentTypes, other.argumentTypes) &&
                Objects.equals(this.variableArity, other.variableArity) &&
                Objects.equals(this.internal, other.internal);
    }

    @Override
    public String toString()
    {
        return (internal ? "%" : "") + name + (typeParameters.isEmpty() ? "" : "<" + Joiner.on(",").join(typeParameters) + ">") + "(" + Joiner.on(",").join(argumentTypes) + "):" + returnType;
    }

    @Nullable
    public Map<String, Type> bindTypeParameters(Type returnType, List<? extends Type> types, boolean allowCoercion, FunctionRegistry functionRegistry, TypeManager typeManager)
    {
        ImmutableList.Builder<TypeSignature> expectedTypeSignaturesBuilder = new ImmutableList.Builder<>();
        expectedTypeSignaturesBuilder.add(this.returnType);
        expectedTypeSignaturesBuilder.addAll(argumentTypes);
        ImmutableList.Builder<TypeSignature> actualTypeSignaturesBuilder = new ImmutableList.Builder<>();
        actualTypeSignaturesBuilder.add(returnType.getTypeSignature());
        types.stream().map(Type::getTypeSignature).forEach(actualTypeSignaturesBuilder::add);

        Map<String, TypeSignature> boundParameters = bindTypeParameters(typeParameters, expectedTypeSignaturesBuilder.build(), actualTypeSignaturesBuilder.build(), allowCoercion, variableArity, typeManager);

        if (boundParameters == null) {
            return null;
        }

        ImmutableMap.Builder<String, Type> resultBuilder = new ImmutableMap.Builder<>();
        for (Map.Entry<String, TypeSignature> entry : boundParameters.entrySet()) {
            resultBuilder.put(entry.getKey(), typeManager.getType(entry.getValue()));
        }
        return resultBuilder.build();
    }

    @Nullable
    public Map<String, Type> bindTypeParameters(List<? extends Type> types, boolean allowCoercion, FunctionRegistry functionRegistry, TypeManager typeManager)
    {
        List<? extends TypeSignature> actualTypeSignatures = types.stream()
                .map(Type::getTypeSignature)
                .collect(ImmutableCollectors.toImmutableList());

        Map<String, TypeSignature> boundParameters = bindTypeParameters(typeParameters, argumentTypes, actualTypeSignatures, allowCoercion, variableArity, typeManager);

        if (boundParameters == null) {
            return null;
        }

        ImmutableMap.Builder<String, Type> resultBuilder = new ImmutableMap.Builder<>();
        for (Map.Entry<String, TypeSignature> entry : boundParameters.entrySet()) {
            resultBuilder.put(entry.getKey(), typeManager.getType(entry.getValue()));
        }
        return resultBuilder.build();
    }

    @Nullable
    private static Map<String, TypeSignature> bindTypeParameters(
            List<? extends TypeParameter> typeParameters,
            List<? extends TypeSignature> expectedTypeSignatures,
            List<? extends TypeSignature> actualTypeSignatures,
            boolean allowCoercion,
            boolean variableArity,
            TypeManager typeManager)
    {
        ImmutableMap.Builder<String, TypeParameter> typeParameterMapBuilder = ImmutableMap.builder();
        for (TypeParameter parameter : typeParameters) {
            typeParameterMapBuilder.put(parameter.getName(), parameter);
        }
        ImmutableMap<String, TypeParameter> typeParameterMap = typeParameterMapBuilder.build();

        TypeUnification.TypeUnificationResult unificationResult = TypeUnification.unify(typeParameterMap, expectedTypeSignatures, actualTypeSignatures, allowCoercion, variableArity, typeManager);
        if (unificationResult == null) {
            return null;
        }
        Map<String, TypeSignature> boundParameters = unificationResult.getResolvedTypeParameters();
        if (!boundParameters.keySet().equals(typeParameterMap.keySet())) {
            return null;
        }
        return boundParameters;
    }

    @Nullable
    public List<? extends TypeSignature> bindUnboundArguments(List<? extends TypeSignature> types, boolean allowCoercion, TypeManager typeManager)
    {
        ImmutableMap.Builder<String, TypeParameter> builder = ImmutableMap.builder();
        for (TypeParameter parameter : typeParameters) {
            builder.put(parameter.getName(), parameter);
        }

        TypeUnification.TypeUnificationResult unificationResult = TypeUnification.unify(builder.build(), argumentTypes, types, allowCoercion, false, typeManager);
        if (unificationResult == null) {
            return null;
        }

        return unificationResult.getResolvedArguments();
    }

    /*
     * similar to T extends MyClass<?...>, if Java supported varargs wildcards
     */
    public static TypeParameter withVariadicBound(String name, String variadicBound)
    {
        return new TypeParameter(name, false, false, variadicBound);
    }

    public static TypeParameter comparableWithVariadicBound(String name, String variadicBound)
    {
        return new TypeParameter(name, true, false, variadicBound);
    }

    public static TypeParameter typeParameter(String name)
    {
        return new TypeParameter(name, false, false, null);
    }

    public static TypeParameter comparableTypeParameter(String name)
    {
        return new TypeParameter(name, true, false, null);
    }

    public static TypeParameter orderableTypeParameter(String name)
    {
        return new TypeParameter(name, false, true, null);
    }
}
