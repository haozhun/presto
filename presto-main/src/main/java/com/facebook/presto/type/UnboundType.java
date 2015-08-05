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
package com.facebook.presto.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.AbstractFixedWidthType;

import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;

public final class UnboundType
        extends AbstractFixedWidthType
{
    public static final UnboundType UNBOUND = new UnboundType();
    public static final String NAME = "unbound";

    private UnboundType()
    {
        super(parameterizedTypeName(NAME), void.class, 0);
    }

    @Override
    public boolean isComparable()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOrderable()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int hash(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        throw new UnsupportedOperationException();
    }
}
