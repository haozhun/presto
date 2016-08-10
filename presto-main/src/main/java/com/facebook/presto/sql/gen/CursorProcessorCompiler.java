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
package com.facebook.presto.sql.gen;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.ForLoop;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.CursorProcessor;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.relational.Signatures;
import com.facebook.presto.sql.relational.VariableReferenceExpression;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;

import java.util.List;

import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.OpCode.NOP;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.sql.gen.BytecodeUtils.generateWrite;
import static com.facebook.presto.sql.gen.TryCodeGenerator.defineTryMethod;
import static com.facebook.presto.sql.gen.TryExpressionExtractor.extractTryOrLambdaExpressions;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;

public class CursorProcessorCompiler
        implements BodyCompiler<CursorProcessor>
{
    private final Metadata metadata;

    public CursorProcessorCompiler(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public void generateMethods(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, RowExpression filter, List<RowExpression> projections)
    {
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);
        generateProcessMethod(classDefinition, projections.size());
        generateFilterMethod(classDefinition, callSiteBinder, cachedInstanceBinder, filter);

        for (int i = 0; i < projections.size(); i++) {
            generateProjectMethod(classDefinition, callSiteBinder, cachedInstanceBinder, "project_" + i, projections.get(i));
        }

        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));
        BytecodeBlock constructorBody = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();
        constructorBody.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);
        cachedInstanceBinder.generateInitializations(thisVariable, constructorBody);
        constructorBody.ret();
    }

    private void generateProcessMethod(ClassDefinition classDefinition, int projections)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter cursor = arg("cursor", RecordCursor.class);
        Parameter count = arg("count", int.class);
        Parameter pageBuilder = arg("pageBuilder", PageBuilder.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "process", type(int.class), session, cursor, count, pageBuilder);

        Scope scope = method.getScope();
        Variable completedPositionsVariable = scope.declareVariable(int.class, "completedPositions");

        method.getBody()
                .comment("int completedPositions = 0;")
                .putVariable(completedPositionsVariable, 0);

        //
        // for loop loop body
        //
        LabelNode done = new LabelNode("done");
        ForLoop forLoop = new ForLoop()
                .initialize(NOP)
                .condition(new BytecodeBlock()
                                .comment("completedPositions < count")
                                .getVariable(completedPositionsVariable)
                                .getVariable(count)
                                .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class)
                )
                .update(new BytecodeBlock()
                                .comment("completedPositions++")
                                .incrementVariable(completedPositionsVariable, (byte) 1)
                );

        BytecodeBlock forLoopBody = new BytecodeBlock()
                .comment("if (pageBuilder.isFull()) break;")
                .append(new BytecodeBlock()
                        .getVariable(pageBuilder)
                        .invokeVirtual(PageBuilder.class, "isFull", boolean.class)
                        .ifTrueGoto(done))
                .comment("if (!cursor.advanceNextPosition()) break;")
                .append(new BytecodeBlock()
                        .getVariable(cursor)
                        .invokeInterface(RecordCursor.class, "advanceNextPosition", boolean.class)
                        .ifFalseGoto(done));

        forLoop.body(forLoopBody);

        // if (filter(cursor))
        IfStatement ifStatement = new IfStatement();
        ifStatement.condition()
                .append(method.getThis())
                .getVariable(session)
                .getVariable(cursor)
                .invokeVirtual(classDefinition.getType(), "filter", type(boolean.class), type(ConnectorSession.class), type(RecordCursor.class));

        // pageBuilder.declarePosition();
        ifStatement.ifTrue()
                .getVariable(pageBuilder)
                .invokeVirtual(PageBuilder.class, "declarePosition", void.class);

        // this.project_43(session, cursor, pageBuilder.getBlockBuilder(42)));
        for (int projectionIndex = 0; projectionIndex < projections; projectionIndex++) {
            ifStatement.ifTrue()
                    .append(method.getThis())
                    .getVariable(session)
                    .getVariable(cursor);

            // pageBuilder.getBlockBuilder(0)
            ifStatement.ifTrue()
                    .getVariable(pageBuilder)
                    .push(projectionIndex)
                    .invokeVirtual(PageBuilder.class, "getBlockBuilder", BlockBuilder.class, int.class);

            // project(block..., blockBuilder)gen
            ifStatement.ifTrue()
                    .invokeVirtual(classDefinition.getType(),
                    "project_" + projectionIndex,
                    type(void.class),
                    type(ConnectorSession.class),
                    type(RecordCursor.class),
                    type(BlockBuilder.class));
        }
        forLoopBody.append(ifStatement);

        method.getBody()
                .append(forLoop)
                .visitLabel(done)
                .comment("return completedPositions;")
                .getVariable(completedPositionsVariable)
                .retInt();
    }

    private PreGeneratedExpressions generateTryMethods(
            ClassDefinition containerClassDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpression projection,
            String methodPrefix)
    {
        List<RowExpression> tryOrLambdaExpressions = extractTryOrLambdaExpressions(projection);

        ImmutableMap.Builder<CallExpression, MethodDefinition> tryMethodMap = ImmutableMap.builder();
        ImmutableMap.Builder<LambdaDefinitionExpression, FieldDefinition> lambdaFieldMap = ImmutableMap.builder();

        for (int i = 0; i < tryOrLambdaExpressions.size(); i++) {
            RowExpression expression = tryOrLambdaExpressions.get(i);

            if (expression instanceof CallExpression) {
                CallExpression tryExpression = (CallExpression) expression;
                verify(!Signatures.TRY.equals(tryExpression.getSignature().getName()));

                Parameter session = arg("session", ConnectorSession.class);
                Parameter cursor = arg("cursor", RecordCursor.class);

                List<Parameter> inputParameters = ImmutableList.<Parameter>builder()
                        .add(session)
                        .add(cursor)
                        .build();

                BytecodeExpressionVisitor innerExpressionVisitor = new BytecodeExpressionVisitor(
                        callSiteBinder,
                        cachedInstanceBinder,
                        fieldReferenceCompiler(),
                        metadata.getFunctionRegistry(),
                        new PreGeneratedExpressions(tryMethodMap.build(), lambdaFieldMap.build()));

                MethodDefinition tryMethod = defineTryMethod(
                        innerExpressionVisitor,
                        containerClassDefinition,
                        methodPrefix + "_try_" + i,
                        inputParameters,
                        Primitives.wrap(tryExpression.getType().getJavaType()),
                        tryExpression,
                        callSiteBinder);

                tryMethodMap.put(tryExpression, tryMethod);
            }
            else if (expression instanceof LambdaDefinitionExpression) {
                LambdaDefinitionExpression lambdaExpression = (LambdaDefinitionExpression) expression;
                String fieldName = methodPrefix + "_lambda_" + i;
                PreGeneratedExpressions preGeneratedExpressions = new PreGeneratedExpressions(tryMethodMap.build(), lambdaFieldMap.build());
                FieldDefinition methodHandleField = LambdaBytecodeGenerator.preGenerateLambdaExpression(
                        lambdaExpression,
                        fieldName,
                        containerClassDefinition,
                        preGeneratedExpressions,
                        callSiteBinder,
                        cachedInstanceBinder,
                        metadata.getFunctionRegistry());
                lambdaFieldMap.put(lambdaExpression, methodHandleField);
            }
            else {
                throw new VerifyException("unexpected expression type in generateTryMethods");
            }
        }

        return new PreGeneratedExpressions(tryMethodMap.build(), lambdaFieldMap.build());
    }

    private void generateFilterMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, CachedInstanceBinder cachedInstanceBinder, RowExpression filter)
    {
        PreGeneratedExpressions preGeneratedExpressions = generateTryMethods(classDefinition, callSiteBinder, cachedInstanceBinder, filter, "filter");

        Parameter session = arg("session", ConnectorSession.class);
        Parameter cursor = arg("cursor", RecordCursor.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "filter", type(boolean.class), session, cursor);

        method.comment("Filter: %s", filter);

        Scope scope = method.getScope();
        Variable wasNullVariable = scope.declareVariable(type(boolean.class), "wasNull");

        BytecodeExpressionVisitor visitor = new BytecodeExpressionVisitor(
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(),
                metadata.getFunctionRegistry(),
                preGeneratedExpressions);

        LabelNode end = new LabelNode("end");
        method.getBody()
                .comment("boolean wasNull = false;")
                .putVariable(wasNullVariable, false)
                .comment("evaluate filter: " + filter)
                .append(filter.accept(visitor, scope))
                .comment("if (wasNull) return false;")
                .getVariable(wasNullVariable)
                .ifFalseGoto(end)
                .pop(boolean.class)
                .push(false)
                .visitLabel(end)
                .retBoolean();
    }

    private void generateProjectMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, CachedInstanceBinder cachedInstanceBinder, String methodName, RowExpression projection)
    {
        PreGeneratedExpressions preGeneratedExpressions = generateTryMethods(classDefinition, callSiteBinder, cachedInstanceBinder, projection, methodName);

        Parameter session = arg("session", ConnectorSession.class);
        Parameter cursor = arg("cursor", RecordCursor.class);
        Parameter output = arg("output", BlockBuilder.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), methodName, type(void.class), session, cursor, output);

        method.comment("Projection: %s", projection.toString());

        Scope scope = method.getScope();
        Variable wasNullVariable = scope.declareVariable(type(boolean.class), "wasNull");

        BytecodeBlock body = method.getBody()
                .comment("boolean wasNull = false;")
                .putVariable(wasNullVariable, false);

        BytecodeExpressionVisitor visitor = new BytecodeExpressionVisitor(
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(),
                metadata.getFunctionRegistry(),
                preGeneratedExpressions);

        body.getVariable(output)
                .comment("evaluate projection: " + projection.toString())
                .append(projection.accept(visitor, scope))
                .append(generateWrite(callSiteBinder, scope, wasNullVariable, projection.getType()))
                .ret();
    }

    private RowExpressionVisitor<Scope, BytecodeNode> fieldReferenceCompiler()
    {
        return new RowExpressionVisitor<Scope, BytecodeNode>()
        {
            @Override
            public BytecodeNode visitInputReference(InputReferenceExpression node, Scope scope)
            {
                int field = node.getField();
                Type type = node.getType();
                Variable cursor = scope.getVariable("cursor");
                Variable wasNull = scope.getVariable("wasNull");

                Class<?> javaType = type.getJavaType();
                if (!javaType.isPrimitive() && javaType != Slice.class) {
                    javaType = Object.class;
                }

                IfStatement ifStatement = new IfStatement();
                ifStatement.condition()
                        .setDescription(format("cursor.get%s(%d)", type, field))
                        .getVariable(cursor)
                        .push(field)
                        .invokeInterface(RecordCursor.class, "isNull", boolean.class, int.class);

                ifStatement.ifTrue()
                        .putVariable(wasNull, true)
                        .pushJavaDefault(javaType);

                ifStatement.ifFalse()
                        .getVariable(cursor)
                        .push(field)
                        .invokeInterface(RecordCursor.class, "get" + Primitives.wrap(javaType).getSimpleName(), javaType, int.class);

                return ifStatement;
            }

            @Override
            public BytecodeNode visitCall(CallExpression call, Scope scope)
            {
                throw new UnsupportedOperationException("not yet implemented");
            }

            @Override
            public BytecodeNode visitConstant(ConstantExpression literal, Scope scope)
            {
                throw new UnsupportedOperationException("not yet implemented");
            }

            @Override
            public BytecodeNode visitLambda(LambdaDefinitionExpression lambda, Scope context)
            {
                throw new UnsupportedOperationException("not yet implemented");
            }

            @Override
            public BytecodeNode visitVariableReference(VariableReferenceExpression reference, Scope context)
            {
                throw new UnsupportedOperationException("not yet implemented");
            }
        };
    }
}
