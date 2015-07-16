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

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.relational.VariableReferenceExpression;
import com.facebook.presto.util.Reflection;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantTrue;
import static com.facebook.presto.byteCode.instruction.Constant.loadBoolean;
import static com.facebook.presto.byteCode.instruction.Constant.loadDouble;
import static com.facebook.presto.byteCode.instruction.Constant.loadFloat;
import static com.facebook.presto.byteCode.instruction.Constant.loadInt;
import static com.facebook.presto.byteCode.instruction.Constant.loadLong;
import static com.facebook.presto.byteCode.instruction.Constant.loadString;
import static com.facebook.presto.sql.gen.ByteCodeUtils.loadConstant;
import static com.facebook.presto.sql.relational.Signatures.CAST;
import static com.facebook.presto.sql.relational.Signatures.COALESCE;
import static com.facebook.presto.sql.relational.Signatures.IF;
import static com.facebook.presto.sql.relational.Signatures.IN;
import static com.facebook.presto.sql.relational.Signatures.IS_NULL;
import static com.facebook.presto.sql.relational.Signatures.NULL_IF;
import static com.facebook.presto.sql.relational.Signatures.SWITCH;

public class ByteCodeExpressionVisitor
        implements RowExpressionVisitor<Scope, ByteCodeNode>
{
    private static final AtomicLong LAMBDA_ID = new AtomicLong();

    private final CallSiteBinder callSiteBinder;
    private final RowExpressionVisitor<Scope, ByteCodeNode> fieldReferenceCompiler;
    private final FunctionRegistry registry;
    private final ClassDefinition classDefinition;

    public ByteCodeExpressionVisitor(
            CallSiteBinder callSiteBinder,
            RowExpressionVisitor<Scope, ByteCodeNode> fieldReferenceCompiler,
            FunctionRegistry registry,
            ClassDefinition classDefinition)
    {
        this.callSiteBinder = callSiteBinder;
        this.fieldReferenceCompiler = fieldReferenceCompiler;
        this.registry = registry;
        this.classDefinition = classDefinition;
    }

    @Override
    public ByteCodeNode visitLambda(LambdaDefinitionExpression lambda, Scope context)
    {
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        for (int i = 0; i < lambda.getArguments().size(); i++) {
            parameters.add(arg(lambda.getArguments().get(i), lambda.getArgumentTypes().get(i).getJavaType()));
        }
        String lambdaName = "lambda" + LAMBDA_ID.getAndIncrement();
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC, STATIC), lambdaName, type(lambda.getBody().getType().getJavaType()), parameters.build());

        Scope scope = method.getScope();
        Variable wasNull = scope.declareVariable(boolean.class, "wasNull");
        ByteCodeExpressionVisitor bodyVisitor = new ByteCodeExpressionVisitor(callSiteBinder, getLamdbaVariableReferenceCompiler(), registry, classDefinition);
        ByteCodeNode compiledBody = lambda.getBody().accept(bodyVisitor, scope);
        method.getBody()
                .putVariable(wasNull, false)
                .append(compiledBody)
                .ret(lambda.getBody().getType().getJavaType());

        Block byteCodeBlock = new Block()
                .push(classDefinition.getType())
                .push(lambdaName)
                .push(lambda.getArguments().size())
                .newArray(Class.class);

        for (int i = 0; i < lambda.getArguments().size(); i++) {
            byteCodeBlock.dup()
                    .push(i)
                    .push(lambda.getArgumentTypes().get(i).getJavaType())
                    .putObjectArrayElement();
        }

        byteCodeBlock.invokeStatic(Reflection.class, "methodHandle", MethodHandle.class, Class.class, String.class, Class[].class);

        return byteCodeBlock;
    }

    public static RowExpressionVisitor<Scope, ByteCodeNode> getLamdbaVariableReferenceCompiler()
    {
        return new RowExpressionVisitor<Scope, ByteCodeNode>() {
            @Override
            public ByteCodeNode visitCall(CallExpression call, Scope context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ByteCodeNode visitLambda(LambdaDefinitionExpression lambda, Scope context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ByteCodeNode visitVariableReference(VariableReferenceExpression reference, Scope context)
            {
                return context.getVariable(reference.getName());
            }

            @Override
            public ByteCodeNode visitInputReference(InputReferenceExpression reference, Scope context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ByteCodeNode visitConstant(ConstantExpression literal, Scope context)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public ByteCodeNode visitCall(CallExpression call, final Scope scope)
    {
        ByteCodeGenerator generator;
        // special-cased in function registry
        if (call.getSignature().getName().equals(CAST)) {
            generator = new CastCodeGenerator();
        }
        else {
            switch (call.getSignature().getName()) {
                // lazy evaluation
                case IF:
                    generator = new IfCodeGenerator();
                    break;
                case NULL_IF:
                    generator = new NullIfCodeGenerator(registry);
                    break;
                case SWITCH:
                    // (SWITCH <expr> (WHEN <expr> <expr>) (WHEN <expr> <expr>) <expr>)
                    generator = new SwitchCodeGenerator();
                    break;
                // functions that take null as input
                case IS_NULL:
                    generator = new IsNullCodeGenerator();
                    break;
                case "IS_DISTINCT_FROM":
                    generator = new IsDistinctFromCodeGenerator();
                    break;
                case COALESCE:
                    generator = new CoalesceCodeGenerator();
                    break;
                // functions that require varargs and/or complex types (e.g., lists)
                case IN:
                    generator = new InCodeGenerator();
                    break;
                // optimized implementations (shortcircuiting behavior)
                case "AND":
                    generator = new AndCodeGenerator();
                    break;
                case "OR":
                    generator = new OrCodeGenerator();
                    break;
                default:
                    generator = new FunctionCallCodeGenerator();
            }
        }

        ByteCodeGeneratorContext generatorContext = new ByteCodeGeneratorContext(
                this,
                scope,
                callSiteBinder,
                registry);

        return generator.generateExpression(call.getSignature(), generatorContext, call.getType(), call.getArguments());
    }

    @Override
    public ByteCodeNode visitConstant(ConstantExpression constant, Scope scope)
    {
        Object value = constant.getValue();
        Class<?> javaType = constant.getType().getJavaType();

        Block block = new Block();
        if (value == null) {
            return block.comment("constant null")
                    .append(scope.getVariable("wasNull").set(constantTrue()))
                    .pushJavaDefault(javaType);
        }

        // use LDC for primitives (boolean, short, int, long, float, double)
        block.comment("constant " + constant.getType().getTypeSignature());
        if (javaType == boolean.class) {
            return block.append(loadBoolean((Boolean) value));
        }
        if (javaType == byte.class || javaType == short.class || javaType == int.class) {
            return block.append(loadInt(((Number) value).intValue()));
        }
        if (javaType == long.class) {
            return block.append(loadLong((Long) value));
        }
        if (javaType == float.class) {
            return block.append(loadFloat((Float) value));
        }
        if (javaType == double.class) {
            return block.append(loadDouble((Double) value));
        }
        if (javaType == String.class) {
            return block.append(loadString((String) value));
        }
        if (javaType == void.class) {
            return block;
        }

        // bind constant object directly into the call-site using invoke dynamic
        Binding binding = callSiteBinder.bind(value, constant.getType().getJavaType());

        return new Block()
                .setDescription("constant " + constant.getType())
                .comment(constant.toString())
                .append(loadConstant(binding));
    }

    @Override
    public ByteCodeNode visitVariableReference(VariableReferenceExpression reference, Scope scope)
    {
        return fieldReferenceCompiler.visitVariableReference(reference, scope);
    }

    @Override
    public ByteCodeNode visitInputReference(InputReferenceExpression node, Scope scope)
    {
        return fieldReferenceCompiler.visitInputReference(node, scope);
    }
}
