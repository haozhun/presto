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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.util.ImmutableCollectors;
import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LambdaCaptureDesugaringRewriter
        extends ExpressionRewriter<Void>
{
    private final IdentityHashMap<Expression, Type> expressionTypes;

    public LambdaCaptureDesugaringRewriter(IdentityHashMap<Expression, Type> expressionTypes)
    {
        this.expressionTypes = requireNonNull(expressionTypes, "expressionTypes is null");
    }

    public class Visitor
            extends ExpressionRewriter<Context>
    {
        @Override
        public Expression rewriteLambdaExpression(LambdaExpression node, Context context, ExpressionTreeRewriter<Context> treeRewriter)
        {
            checkState(node.isBodyContainsSymbolReferences());

            HashSet<Symbol> symbols = context.getSymbols();

            HashSet<Symbol> bodyReferencedSymbols = new HashSet<>();
            context.setSymbols(bodyReferencedSymbols);
            Expression body = treeRewriter.rewrite(node.getBody(), context);

            ImmutableList<Symbol> lambdaArguments = node.getArgumentNames().stream()
                    .map(Symbol::new)
                    .collect(toImmutableList());

            // bodyReferencedSymbols is no longer meaningful after this line
            Set<Symbol> extraLambdaArguments = bodyReferencedSymbols;
            extraLambdaArguments.removeAll(lambdaArguments);
            ImmutableList<String> extraLambdaArgumentNames = extraLambdaArguments.stream()
                    .map(Symbol::getName)
                    .collect(toImmutableList());
            ImmutableList<String> newLambdaArgumentNames = ImmutableList.<String>builder()
                    .addAll(extraLambdaArgumentNames)
                    .addAll(node.getArgumentNames())
                    .build();
            ImmutableList<Expression> expressionsToBind = extraLambdaArguments.stream()
                    .map(Symbol::toSymbolReference)
                    .collect(toImmutableList());

            LambdaExpression lambdaExpression = new LambdaExpression(newLambdaArgumentNames, body, true);





            // TODO: add any partially_binded symbols to symbols

            context.setSymbols(symbols);
            return result;
        }
    }

    private static class Context {
        HashSet<Symbol> symbols;

        public HashSet<Symbol> getSymbols()
        {
            return symbols;
        }

        public void setSymbols(HashSet<Symbol> symbols)
        {
            this.symbols = symbols;
        }
    }
}
