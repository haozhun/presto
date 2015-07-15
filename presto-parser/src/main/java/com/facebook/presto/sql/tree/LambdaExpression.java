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
package com.facebook.presto.sql.tree;

import java.util.List;
import java.util.Objects;

public class LambdaExpression
        extends Expression
{
    private final List<QualifiedName> name;
    private final Expression expression;

    public LambdaExpression(List<QualifiedName> name, Expression expression)
    {
        this.name = name;
        this.expression = expression;
    }

    public List<QualifiedName> getArguments()
    {
        return name;
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLambdaExpression(this, context);
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
        LambdaExpression that = (LambdaExpression) obj;
        return Objects.equals(name, that.name) &&
                Objects.equals(expression, that.expression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, expression);
    }
}
