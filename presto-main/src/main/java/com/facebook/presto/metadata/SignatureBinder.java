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

import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.ParameterKind;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.type.FunctionType;
import com.facebook.presto.type.TypeRegistry;
import com.facebook.presto.util.ImmutableCollectors;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.type.TypeCalculation.calculateLiteralValue;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * TODO: document example cases that doesn't work
 */
public class SignatureBinder
{
    private static final int SOLVE_ITERATION_LIMIT = 4;

    private final TypeManager typeManager;
    private final Signature declaredSignature;
    private final boolean allowCoercion;
    private final Map<String, TypeVariableConstraint> typeVariableConstraints;

    public SignatureBinder(TypeManager typeManager, Signature declaredSignature, boolean allowCoercion)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.declaredSignature = requireNonNull(declaredSignature, "parametrizedSignature is null");
        this.allowCoercion = allowCoercion;
        this.typeVariableConstraints = declaredSignature.getTypeVariableConstraints()
                .stream()
                .collect(toMap(TypeVariableConstraint::getName, t -> t));
    }

    private boolean toSolvers(
            ImmutableList.Builder<TypeConstraintSolver> resultBuilder,
            Signature targetSignature,
            List<? extends TypeSignatureProvider> actualTypes,
            Type exactReturnType)
    {
        Map<String, TypeVariableConstraint> typeVariableMap = targetSignature.getTypeVariableConstraints().stream()
                .collect(ImmutableCollectors.toImmutableMap(TypeVariableConstraint::getName, Function.identity()));

        if (!toSolver(resultBuilder, typeVariableMap, targetSignature.getReturnType(), new TypeSignatureProvider(exactReturnType.getTypeSignature()), false)) {
            return false;
        }

        return toSolvers(resultBuilder, typeVariableMap, actualTypes);
    }

    private boolean toSolvers(
            ImmutableList.Builder<TypeConstraintSolver> resultBuilder,
            Signature targetSignature,
            List<? extends TypeSignatureProvider> actualTypes)
    {
        Map<String, TypeVariableConstraint> typeVariableMap = targetSignature.getTypeVariableConstraints().stream()
                .collect(ImmutableCollectors.toImmutableMap(TypeVariableConstraint::getName, Function.identity()));
        return toSolvers(resultBuilder, typeVariableMap, actualTypes);
    }

    private boolean toSolvers(
            ImmutableList.Builder<TypeConstraintSolver> resultBuilder,
            Map<String, TypeVariableConstraint> typeVariableMap,
            List<? extends TypeSignatureProvider> actualTypes)
    {
        boolean variableArity = declaredSignature.isVariableArity();
        List<TypeSignature> formalTypeSignatures = declaredSignature.getArgumentTypes();
        if (variableArity) {
            if (actualTypes.size() < formalTypeSignatures.size() - 1) {
                return false;
            }
            formalTypeSignatures = expandVarargFormalTypeSignature(formalTypeSignatures, actualTypes.size());
        }

        return toSolvers(resultBuilder, typeVariableMap, formalTypeSignatures, actualTypes, allowCoercion);
    }

    private boolean toSolvers(
            ImmutableList.Builder<TypeConstraintSolver> resultBuilder,
            Map<String, TypeVariableConstraint> typeVariableMap,
            List<? extends TypeSignature> formalTypeSignatures,
            List<? extends TypeSignatureProvider> actualTypes,
            boolean allowCoercion)
    {
        if (formalTypeSignatures.size() != actualTypes.size()) {
            return false;
        }

        for (int i = 0; i < formalTypeSignatures.size(); i++) {
            TypeSignature formalTypeSignature = formalTypeSignatures.get(i);
            TypeSignatureProvider actualType = actualTypes.get(i);
            if (!toSolver(resultBuilder, typeVariableMap, formalTypeSignature, actualType, allowCoercion)) {
                return false;
            }
        }
        return true;
    }

    private boolean toSolver(
            ImmutableList.Builder<TypeConstraintSolver> resultBuilder,
            Map<String, TypeVariableConstraint> typeVariableMap,
            TypeSignature formalTypeSignature,
            TypeSignatureProvider actualTypeSignatureProvider,
            boolean allowCoercion)
    {
        List<TypeSignatureParameter> formalTypeSignatureParameters = formalTypeSignature.getParameters();
        if (formalTypeSignatureParameters.isEmpty()) {
            Type actualType = typeManager.getType(actualTypeSignatureProvider.getTypeSignature());
            TypeVariableConstraint typeVariableConstraint = typeVariableMap.get(formalTypeSignature.getBase());
            if (typeVariableConstraint != null) {
                resultBuilder.add(
                        new TypeParameterSolver(
                                formalTypeSignature.getBase(),
                                actualType,
                                allowCoercion,
                                typeVariableConstraint.isComparableRequired(),
                                typeVariableConstraint.isOrderableRequired(),
                                Optional.ofNullable(typeVariableConstraint.getVariadicBound())));
                return true;
            }
            resultBuilder.add(new FixTypeSolver(formalTypeSignature, actualType, allowCoercion));
            return true;
        }

        switch (getParameterKind(formalTypeSignature)) {
            case LONG: {
                Type actualType = typeManager.getType(actualTypeSignatureProvider.getTypeSignature());
                resultBuilder.add(new FixTypeSolver(formalTypeSignature, actualType, allowCoercion));
                return true;
            }
            case VARIABLE: {
                Type actualType = typeManager.getType(actualTypeSignatureProvider.getTypeSignature());
                resultBuilder.add(new LiteralSolver(formalTypeSignature, actualType, allowCoercion));
                return true;
            }
            case TYPE: {
                if (!FunctionType.NAME.equals(formalTypeSignature.getBase())) {
                    checkArgument(!actualTypeSignatureProvider.hasDependency());
                    Type actualType = typeManager.getType(actualTypeSignatureProvider.getTypeSignature());

                    ImmutableList.Builder<TypeConstraintSolver> solverBuilder = ImmutableList.builder();
                    List<TypeSignatureProvider> actualTypeParametersTypeSignatureProvider;
                    if (UNKNOWN.equals(actualType)) {
                        TypeSignatureProvider unknownTypeSignatureProvider = new TypeSignatureProvider(UNKNOWN.getTypeSignature());
                        actualTypeParametersTypeSignatureProvider = Collections.nCopies(formalTypeSignature.getParameters().size(), unknownTypeSignatureProvider);
                    }
                    else {
                        actualTypeParametersTypeSignatureProvider = fromTypes(actualType.getTypeParameters());
                    }

                    ImmutableList.Builder<TypeSignature> formalTypeParameterTypeSignaturesBuilder = ImmutableList.builder();
                    ImmutableList.Builder<Optional<String>> namesBuilder = ImmutableList.builder();
                    for (TypeSignatureParameter formalTypeParameter : formalTypeSignatureParameters) {
                        switch (formalTypeParameter.getKind()) {
                            case NAMED_TYPE:
                                NamedTypeSignature namedType = formalTypeParameter.getNamedTypeSignature();
                                formalTypeParameterTypeSignaturesBuilder.add(namedType.getTypeSignature());
                                namesBuilder.add(Optional.of(namedType.getName()));
                                break;
                            case TYPE:
                                formalTypeParameterTypeSignaturesBuilder.add(formalTypeParameter.getTypeSignature());
                                namesBuilder.add(Optional.empty());
                                break;
                            default:
                                throw new IllegalArgumentException();
                        }
                    }

                    if (!toSolvers(
                            solverBuilder,
                            typeVariableMap,
                            formalTypeParameterTypeSignaturesBuilder.build(),
                            actualTypeParametersTypeSignatureProvider,
                            allowCoercion && TypeRegistry.isCovariantTypeBase(formalTypeSignature.getBase()))) {
                        return false;
                    }
                    resultBuilder.add(new ListSolver(formalTypeSignature, actualType, solverBuilder.build(), allowCoercion));
                    return true;
                }
                else {
                    List<TypeSignature> formalTypeParameterTypeSignatures = formalTypeSignature.getTypeParametersAsTypeSignatures();
                    FunctionSolver functionSolver = new FunctionSolver(
                            formalTypeParameterTypeSignatures.subList(0, formalTypeParameterTypeSignatures.size() - 1),
                            formalTypeParameterTypeSignatures.get(formalTypeParameterTypeSignatures.size() - 1),
                            actualTypeSignatureProvider);

                    resultBuilder.add(functionSolver);
                    return true;
                }
            }
            default:
                throw new IllegalArgumentException();
        }
    }

    private ParameterKind getParameterKind(TypeSignature typeSignature)
    {
        ParameterKind result = null;
        for (TypeSignatureParameter typeSignatureParameter : typeSignature.getParameters()) {
            switch (typeSignatureParameter.getKind()) {
                case LONG:
                    if (result == null) {
                        result = ParameterKind.LONG;
                    }
                    else {
                        checkState(result == ParameterKind.VARIABLE || result == ParameterKind.LONG);
                    }
                    break;
                case VARIABLE:
                    if (result == null || result == ParameterKind.LONG) {
                        result = ParameterKind.VARIABLE;
                    }
                    else {
                        checkState(result == ParameterKind.VARIABLE);
                    }
                    break;
                case TYPE:
                case NAMED_TYPE:
                    if (result == null) {
                        result = ParameterKind.TYPE;
                    }
                    else {
                        checkState(result == ParameterKind.TYPE);
                    }
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        }
        return result;
    }

    public Optional<Signature> bind(List<? extends TypeSignatureProvider> actualArgumentTypes)
    {
        Optional<BoundVariables> boundVariables = bindVariables(actualArgumentTypes);
        if (!boundVariables.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(applyBoundVariables(declaredSignature, boundVariables.get(), actualArgumentTypes.size()));
    }

    public Optional<BoundVariables> bindVariables(List<? extends TypeSignatureProvider> actualArgumentTypes)
    {
        ImmutableList.Builder<TypeConstraintSolver> solversBuilder = ImmutableList.builder();
        if (!toSolvers(solversBuilder, declaredSignature, actualArgumentTypes)) {
            return Optional.empty();
        }
        ImmutableList<TypeConstraintSolver> solvers = solversBuilder.build();

        BoundVariables.Builder boundVariablesBuilder = BoundVariables.builder();
        for (int i = 0; /*no-op*/; i++) {
            if (i == SOLVE_ITERATION_LIMIT) {
                throw new VerifyException(format("SignatureBinder.solve does not converge after %d iterations.", SOLVE_ITERATION_LIMIT));
            }
            boolean changed = false;
            for (TypeConstraintSolver constraint : solvers) {
                SolverReturnStatus status = constraint.update(boundVariablesBuilder);
                if (status == SolverReturnStatus.UNSOLVABLE) {
                    return Optional.empty();
                }
                if (status == SolverReturnStatus.CHANGED) {
                    changed = true;
                }
            }
            if (!changed) {
                break;
            }
        }

        calculateVariableValuesForLongConstraints(boundVariablesBuilder);
        BoundVariables boundVariables = boundVariablesBuilder.build();
        if (!allTypeVariablesBound(boundVariables)) {
            return Optional.empty();
        }
        return Optional.of(boundVariables);
    }

    public Optional<BoundVariables> bindVariables(List<? extends TypeSignatureProvider> actualArgumentTypes, Type actualReturnType)
    {
        ImmutableList.Builder<TypeConstraintSolver> solversBuilder = ImmutableList.builder();
        if (!toSolvers(solversBuilder, declaredSignature, actualArgumentTypes, actualReturnType)) {
            return Optional.empty();
        }
        ImmutableList<TypeConstraintSolver> solvers = solversBuilder.build();

        BoundVariables.Builder boundVariablesBuilder = BoundVariables.builder();
        boolean changed;
        do {
            changed = false;
            for (TypeConstraintSolver solver : solvers) {
                SolverReturnStatus solverStatus = solver.update(boundVariablesBuilder);
                if (solverStatus == SolverReturnStatus.UNSOLVABLE) {
                    return Optional.empty();
                }
                if (solverStatus == SolverReturnStatus.CHANGED) {
                    changed = true;
                }
            }
        }
        while (changed);

        calculateVariableValuesForLongConstraints(boundVariablesBuilder);
        BoundVariables boundVariables = boundVariablesBuilder.build();
        if (!allTypeVariablesBound(boundVariables)) {
            return Optional.empty();
        }
        return Optional.of(boundVariables);
    }

    private void calculateVariableValuesForLongConstraints(BoundVariables.Builder variableBinder)
    {
        for (LongVariableConstraint longVariableConstraint : declaredSignature.getLongVariableConstraints()) {
            String calculation = longVariableConstraint.getExpression();
            String variableName = longVariableConstraint.getName();
            Long calculatedValue = calculateLiteralValue(calculation, variableBinder.getLongVariables());
            if (variableBinder.containsLongVariable(variableName)) {
                Long currentValue = variableBinder.getLongVariable(variableName);
                checkState(Objects.equals(currentValue, calculatedValue),
                        "variable '%s' is already set to %s when trying to set %s", variableName, currentValue, calculatedValue);
            }
            variableBinder.setLongVariable(variableName, calculatedValue);
        }
    }

    private boolean allTypeVariablesBound(BoundVariables boundVariables)
    {
        return boundVariables.getTypeVariables().keySet().equals(typeVariableConstraints.keySet());
    }

    public static Signature applyBoundVariables(Signature signature, BoundVariables boundVariables, int arity)
    {
        List<TypeSignature> argumentSignatures = expandVarargFormalTypeSignature(signature.getArgumentTypes(), arity);
        List<TypeSignature> boundArgumentSignatures = applyBoundVariables(argumentSignatures, boundVariables);
        TypeSignature boundReturnTypeSignature = applyBoundVariables(signature.getReturnType(), boundVariables);

        return new Signature(
                signature.getName(),
                signature.getKind(),
                ImmutableList.of(),
                ImmutableList.of(),
                boundReturnTypeSignature,
                boundArgumentSignatures,
                false);
    }

    public static List<TypeSignature> applyBoundVariables(List<TypeSignature> typeSignatures, BoundVariables boundVariables)
    {
        ImmutableList.Builder<TypeSignature> builder = ImmutableList.builder();
        for (TypeSignature typeSignature : typeSignatures) {
            builder.add(applyBoundVariables(typeSignature, boundVariables));
        }
        return builder.build();
    }

    public static TypeSignature applyBoundVariables(TypeSignature typeSignature, BoundVariables boundVariables)
    {
        String baseType = typeSignature.getBase();
        if (boundVariables.containsTypeVariable(baseType)) {
            checkState(typeSignature.getParameters().isEmpty(), "Type parameters cannot have parameters");
            return boundVariables.getTypeVariable(baseType).getTypeSignature();
        }

        List<TypeSignatureParameter> parameters = typeSignature.getParameters().stream()
                .map(typeSignatureParameter -> applyBoundVariables(typeSignatureParameter, boundVariables))
                .collect(toList());

        return new TypeSignature(baseType, parameters);
    }

    private static TypeSignatureParameter applyBoundVariables(TypeSignatureParameter parameter, BoundVariables boundVariables)
    {
        ParameterKind parameterKind = parameter.getKind();
        switch (parameterKind) {
            case TYPE: {
                TypeSignature typeSignature = parameter.getTypeSignature();
                return TypeSignatureParameter.of(applyBoundVariables(typeSignature, boundVariables));
            }
            case NAMED_TYPE: {
                NamedTypeSignature namedTypeSignature = parameter.getNamedTypeSignature();
                TypeSignature typeSignature = namedTypeSignature.getTypeSignature();
                return TypeSignatureParameter.of(new NamedTypeSignature(
                        namedTypeSignature.getName(),
                        applyBoundVariables(typeSignature, boundVariables)));
            }
            case VARIABLE: {
                String variableName = parameter.getVariable();
                checkState(boundVariables.containsLongVariable(variableName),
                        "Variable is not bound: %s", variableName);
                Long variableValue = boundVariables.getLongVariable(variableName);
                return TypeSignatureParameter.of(variableValue);
            }
            case LONG: {
                return parameter;
            }
            default:
                throw new IllegalStateException("Unknown parameter kind: " + parameter.getKind());
        }
    }

    private static List<TypeSignature> expandVarargFormalTypeSignature(List<TypeSignature> formalTypeSignatures, int actualArity)
    {
        int variableArityArgumentsCount = actualArity - formalTypeSignatures.size();
        if (variableArityArgumentsCount == -1) {
            return formalTypeSignatures.subList(0, formalTypeSignatures.size() - 1);
        }
        if (variableArityArgumentsCount == 0) {
            return formalTypeSignatures;
        }
        checkArgument(variableArityArgumentsCount > 0 && !formalTypeSignatures.isEmpty());

        ImmutableList.Builder<TypeSignature> builder = ImmutableList.builder();
        builder.addAll(formalTypeSignatures);
        TypeSignature lastTypeSignature = formalTypeSignatures.get(formalTypeSignatures.size() - 1);
        for (int i = 0; i < variableArityArgumentsCount; i++) {
            builder.add(lastTypeSignature);
        }
        return builder.build();
    }

    private interface TypeConstraintSolver
    {
        SolverReturnStatus update(BoundVariables.Builder bindings);
    }

    private enum SolverReturnStatus
    {
        UNSOLVABLE,
        CHANGED,
        UNCHANGED_NOT_SATISFIED,
        UNCHANGED,
    }

    private class TypeParameterSolver
            implements TypeConstraintSolver
    {
        private final String typeParameter;
        private final Type actualType;
        private final boolean allowCoercion;
        private final boolean comparableRequired;
        private final boolean orderableRequired;
        private final Optional<String> requiredBaseName;

        public TypeParameterSolver(String typeParameter, Type actualType, boolean allowCoercion, boolean comparableRequired, boolean orderableRequired, Optional<String> requiredBaseName)
        {
            this.typeParameter = typeParameter;
            this.actualType = actualType;
            this.allowCoercion = allowCoercion;
            this.comparableRequired = comparableRequired;
            this.orderableRequired = orderableRequired;
            this.requiredBaseName = requiredBaseName;
        }

        @Override
        public SolverReturnStatus update(BoundVariables.Builder bindings)
        {
            Type originalType = bindings.getTypeVariable(typeParameter);
            if (originalType == null) {
                if (!satisfiesConstraints(actualType)) {
                    return SolverReturnStatus.UNSOLVABLE;
                }
                bindings.setTypeVariable(typeParameter, actualType);
                return SolverReturnStatus.CHANGED;
            }
            Optional<Type> commonSuperType = typeManager.getCommonSuperType(originalType, actualType);
            if (!commonSuperType.isPresent()) {
                return SolverReturnStatus.UNSOLVABLE;
            }
            if (!satisfiesConstraints(commonSuperType.get())) {
                // This check must not be skipped even if commonSuperType is equal to originalType
                return SolverReturnStatus.UNSOLVABLE;
            }
            if (commonSuperType.get().equals(originalType)) {
                return SolverReturnStatus.UNCHANGED;
            }
            bindings.setTypeVariable(typeParameter, commonSuperType.get());
            return SolverReturnStatus.CHANGED;
        }

        private boolean satisfiesConstraints(Type type)
        {
            if (!allowCoercion && !type.equals(actualType)) {
                return false;
            }
            if (comparableRequired && !type.isComparable()) {
                return false;
            }
            if (orderableRequired && !type.isOrderable()) {
                return false;
            }
            if (requiredBaseName.isPresent() && !UNKNOWN.equals(actualType) && !requiredBaseName.get().equals(type.getTypeSignature().getBase())) {
                // TODO: the case below should be properly handled:
                // * `type` does not have the `requiredBaseName` but can be coerced to some type that has the `requiredBaseName`.
                return false;
            }
            return true;
        }
    }

    private class LiteralSolver
            implements TypeConstraintSolver
    {
        private final TypeSignature formalTypeSignature;
        private final Type actualType;
        private final boolean allowCoercion;

        public LiteralSolver(TypeSignature formalTypeSignature, Type actualType, boolean allowCoercion)
        {
            this.formalTypeSignature = formalTypeSignature;
            this.actualType = actualType;
            this.allowCoercion = allowCoercion;
        }

        @Override
        public SolverReturnStatus update(BoundVariables.Builder bindings)
        {
            ImmutableList.Builder<TypeSignatureParameter> originalTypeTypeParametersBuilder = ImmutableList.builder();
            List<TypeSignatureParameter> parameters = formalTypeSignature.getParameters();
            boolean originalTypeSolelyFromBindings = true;
            for (int i = 0; i < parameters.size(); i++) {
                TypeSignatureParameter typeSignatureParameter = parameters.get(i);
                if (typeSignatureParameter.getKind() == ParameterKind.VARIABLE) {
                    Long typeParameter = bindings.getLongVariable(typeSignatureParameter.getVariable());
                    if (typeParameter == null) {
                        // if an existing value doesn't exist for the given variable name, use the value that comes from the actual type.
                        Optional<Type> type = typeManager.coerceTypeBase(actualType, formalTypeSignature.getBase());
                        if (!type.isPresent()) {
                            return SolverReturnStatus.UNSOLVABLE;
                        }
                        TypeSignature typeSignature = type.get().getTypeSignature();
                        originalTypeTypeParametersBuilder.add(TypeSignatureParameter.of(typeSignature.getParameters().get(i).getLongLiteral()));
                        originalTypeSolelyFromBindings = false;
                    }
                    else {
                        originalTypeTypeParametersBuilder.add(TypeSignatureParameter.of(typeParameter));
                    }
                }
                else {
                    verify(typeSignatureParameter.getKind() == ParameterKind.LONG);
                    originalTypeTypeParametersBuilder.add(typeSignatureParameter);
                }
            }
            Type originalType = typeManager.getType(new TypeSignature(formalTypeSignature.getBase(), originalTypeTypeParametersBuilder.build()));
            Optional<Type> commonSuperType = typeManager.getCommonSuperType(originalType, actualType);
            if (!commonSuperType.isPresent()) {
                return SolverReturnStatus.UNSOLVABLE;
            }
            if (!allowCoercion && !actualType.equals(commonSuperType.get())) {
                // This check must not be skipped even if commonSuperType is equal to originalType
                return SolverReturnStatus.UNSOLVABLE;
            }
            if (originalTypeSolelyFromBindings && commonSuperType.get().equals(originalType)) {
                return SolverReturnStatus.UNCHANGED;
            }
            TypeSignature commonSuperTypeSignature = commonSuperType.get().getTypeSignature();
            if (!commonSuperTypeSignature.getBase().equals(formalTypeSignature.getBase())) {
                return SolverReturnStatus.UNSOLVABLE;
            }
            SolverReturnStatus result = SolverReturnStatus.UNCHANGED;
            for (int i = 0; i < parameters.size(); i++) {
                TypeSignatureParameter typeSignatureParameter = parameters.get(i);
                long commonSuperLongLiteral = commonSuperTypeSignature.getParameters().get(i).getLongLiteral();
                if (typeSignatureParameter.getKind() == ParameterKind.VARIABLE) {
                    String variableName = typeSignatureParameter.getVariable();
                    Long typeParameter = bindings.getLongVariable(variableName);
                    if (typeParameter == null || typeParameter != commonSuperLongLiteral) {
                        bindings.setLongVariable(variableName, commonSuperLongLiteral);
                        result = SolverReturnStatus.CHANGED;
                    }
                }
                else {
                    verify(typeSignatureParameter.getKind() == ParameterKind.LONG);
                    if (commonSuperLongLiteral != typeSignatureParameter.getLongLiteral()) {
                        return SolverReturnStatus.UNSOLVABLE;
                    }
                }
            }
            return result;
        }
    }

    private class ListSolver
            implements TypeConstraintSolver
    {
        private final TypeSignature formalTypeSignature;
        private final Type actualType;
        private final List<TypeConstraintSolver> solvers;
        private final boolean allowCoercion;

        public ListSolver(TypeSignature formalTypeSignature, Type actualType, List<TypeConstraintSolver> solvers, boolean allowCoercion)
        {
            this.formalTypeSignature = formalTypeSignature;
            this.actualType = actualType;
            this.solvers = solvers;
            this.allowCoercion = allowCoercion;
        }

        @Override
        public SolverReturnStatus update(BoundVariables.Builder bindings)
        {
            SolverReturnStatus result = SolverReturnStatus.UNCHANGED;
            for (TypeConstraintSolver solver : solvers) {
                SolverReturnStatus subResult = solver.update(bindings);
                switch (subResult) {
                    case UNCHANGED:
                        break;
                    case CHANGED:
                        result = SolverReturnStatus.CHANGED;
                        break;
                    case UNSOLVABLE:
                        return SolverReturnStatus.UNSOLVABLE;
                    default:
                        throw new IllegalArgumentException();
                }
            }

            if (allowCoercion) {
                //TODO remove this special UNKNOWN check
                if (!UNKNOWN.equals(actualType)) {
                    Optional<Type> coercedType = typeManager.coerceTypeBase(actualType, formalTypeSignature.getBase());
                    if (!coercedType.isPresent()) {
                        return SolverReturnStatus.UNSOLVABLE;
                    }
                }
            }
            else {
                if (!actualType.getTypeSignature().getBase().equals(formalTypeSignature.getBase())) {
                    return SolverReturnStatus.UNSOLVABLE;
                }
            }
            return result;
        }
    }

    private class FixTypeSolver
            implements TypeConstraintSolver
    {
        private final TypeSignature formalTypeSignature;
        private final Type actualType;
        private final boolean allowCoercion;

        public FixTypeSolver(TypeSignature formalTypeSignature, Type actualType, boolean allowCoercion)
        {
            this.formalTypeSignature = formalTypeSignature;
            this.actualType = actualType;
            this.allowCoercion = allowCoercion;
        }

        @Override
        public SolverReturnStatus update(BoundVariables.Builder bindings)
        {
            if (allowCoercion) {
                if (typeManager.canCoerce(actualType, typeManager.getType(formalTypeSignature))) {
                    return SolverReturnStatus.UNCHANGED;
                }
                return SolverReturnStatus.UNSOLVABLE;
            }
            if (actualType.getTypeSignature().equals(formalTypeSignature)) {
                return SolverReturnStatus.UNCHANGED;
            }
            return SolverReturnStatus.UNSOLVABLE;
        }
    }

    private class FunctionSolver
            implements TypeConstraintSolver
    {
        private final List<TypeSignature> formalLambdaArgumentsTypeSignature;
        private final TypeSignature formalLambdaReturnTypeSignature;
        private final TypeSignatureProvider typeSignatureProvider;
        // checkArgument: typeSignature that is in typeVariableConstraints must not have type parameters

        public FunctionSolver(
                List<TypeSignature> formalLambdaArgumentsTypeSignature,
                TypeSignature formalLambdaReturnTypeSignature,
                TypeSignatureProvider typeSignatureProvider)
        {
            this.formalLambdaArgumentsTypeSignature = formalLambdaArgumentsTypeSignature;
            this.formalLambdaReturnTypeSignature = formalLambdaReturnTypeSignature;
            this.typeSignatureProvider = typeSignatureProvider;
        }

        @Override
        public SolverReturnStatus update(BoundVariables.Builder bindings)
        {
            ImmutableList.Builder<Type> lambdaArgumentTypesBuilder = ImmutableList.builder();
            for (TypeSignature lambdaArgument : formalLambdaArgumentsTypeSignature) {
                if (typeVariableConstraints.containsKey(lambdaArgument.getBase())) {
                    if (!bindings.containsTypeVariable(lambdaArgument.getBase())) {
                        return SolverReturnStatus.UNCHANGED_NOT_SATISFIED;
                    }
                    Type typeVariable = bindings.getTypeVariable(lambdaArgument.getBase());
                    lambdaArgumentTypesBuilder.add(typeVariable);
                }
                else {
                    lambdaArgumentTypesBuilder.add(typeManager.getType(lambdaArgument));
                }
            }
            ImmutableList<Type> lambdaArgumentTypes = lambdaArgumentTypesBuilder.build();
            TypeSignature actualLambdaTypeSignature = typeSignatureProvider.getTypeSignature(lambdaArgumentTypes);
            Type actualLambdaType = typeManager.getType(actualLambdaTypeSignature);
            Type actualReturnType = ((FunctionType) actualLambdaType).getReturnType();

            // Coercion for the return type, if needed, will be added in the lambda function body
            if (typeVariableConstraints.containsKey(formalLambdaReturnTypeSignature.getBase())) {
                // is type variable
                String returnTypeVariableName = formalLambdaReturnTypeSignature.getBase();
                TypeVariableConstraint returnTypeVariableConstraint = typeVariableConstraints.get(returnTypeVariableName);
                Type originalReturnType = bindings.getTypeVariable(returnTypeVariableName);
                if (originalReturnType == null) {
                    if (!satisfiesConstraints(actualReturnType, actualReturnType, returnTypeVariableConstraint)) {
                        return SolverReturnStatus.UNSOLVABLE;
                    }
                    bindings.setTypeVariable(returnTypeVariableName, actualReturnType);
                    return SolverReturnStatus.CHANGED;
                }
                Optional<Type> commonSuperType = typeManager.getCommonSuperType(originalReturnType, actualReturnType);
                if (!commonSuperType.isPresent()) {
                    return SolverReturnStatus.UNSOLVABLE;
                }
                if (!satisfiesConstraints(commonSuperType.get(), actualReturnType, returnTypeVariableConstraint)) {
                    // This check must not be skipped even if commonSuperType is equal to originalReturnType
                    return SolverReturnStatus.UNSOLVABLE;
                }
                if (commonSuperType.get().equals(originalReturnType)) {
                    return SolverReturnStatus.UNCHANGED;
                }
                bindings.setTypeVariable(returnTypeVariableName, commonSuperType.get());
                return SolverReturnStatus.CHANGED;
            }
            else {
                // is concrete type
                Type formalReturnType = typeManager.getType(formalLambdaReturnTypeSignature);

                if (allowCoercion) {
                    if (typeManager.canCoerce(actualReturnType, formalReturnType)) {
                        return SolverReturnStatus.UNCHANGED;
                    }
                    return SolverReturnStatus.UNSOLVABLE;
                }
                if (actualReturnType.equals(formalReturnType)) {
                    return SolverReturnStatus.UNCHANGED;
                }
                return SolverReturnStatus.UNSOLVABLE;
            }
        }

        private boolean satisfiesConstraints(Type type, Type actualType, TypeVariableConstraint typeVariableConstraint)
        {
            if (!allowCoercion && !type.equals(actualType)) {
                return false;
            }
            if (typeVariableConstraint.isComparableRequired() && !type.isComparable()) {
                return false;
            }
            if (typeVariableConstraint.isOrderableRequired() && !type.isOrderable()) {
                return false;
            }
            if (typeVariableConstraint.getVariadicBound() != null && !UNKNOWN.equals(actualType) && !typeVariableConstraint.getVariadicBound().equals(type.getTypeSignature().getBase())) {
                // TODO: the case below should be properly handled:
                // * `type` does not have the `requiredBaseName` but can be coerced to some type that has the `requiredBaseName`.
                return false;
            }
            return true;
        }
    }
}
