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

import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.relational.VariableReferenceExpression;

public class NoAccessFieldReferenceCompiler
        implements RowExpressionVisitor<Scope, BytecodeNode>
{
    public static final NoAccessFieldReferenceCompiler NO_ACCESS_FIELD_REFERENCE_COMPILER = new NoAccessFieldReferenceCompiler();

    @Override
    public BytecodeNode visitCall(CallExpression call, Scope context)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BytecodeNode visitInputReference(InputReferenceExpression reference, Scope context)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BytecodeNode visitConstant(ConstantExpression literal, Scope context)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BytecodeNode visitLambda(LambdaDefinitionExpression lambda, Scope context)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BytecodeNode visitVariableReference(VariableReferenceExpression reference, Scope context)
    {
        throw new UnsupportedOperationException();
    }
}
