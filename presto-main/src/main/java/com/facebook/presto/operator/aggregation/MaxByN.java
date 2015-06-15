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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.ParametricAggregation;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.facebook.presto.operator.aggregation.state.MaxOrMinByNState;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.util.array.LongBigArray;
import com.facebook.presto.util.array.ObjectBigArray;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.lang.invoke.MethodHandle;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.type.TypeUtils.readStructuralBlock;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkState;

public class MaxByN
        extends ParametricAggregation
{
    public static final MaxByN MAX_BY_N = new MaxByN();
    private static final String NAME = "max_by";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(MaxByN.class, "inputLongLong", MethodHandle.class, MaxOrMinByNState.class, long.class, long.class, long.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(MaxByN.class, "combine", MethodHandle.class, MaxOrMinByNState.class, MaxOrMinByNState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(MaxByN.class, "outputLong", Type.class, MaxOrMinByNState.class, BlockBuilder.class);
    private static final Signature SIGNATURE = new Signature(
            NAME,
            ImmutableList.of(orderableTypeParameter("K"), typeParameter("V")),
            "array<V>",
            ImmutableList.of("V", "K", StandardTypes.BIGINT),
            false,
            false);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public String getDescription()
    {
        return "Returns the values of the first argument, associated with the N maximum values of the second argument";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = types.get("K");
        Type valueType = types.get("V");
        Signature signature = new Signature(
                NAME,
                parameterizedTypeName("array", valueType.getTypeSignature()),
                valueType.getTypeSignature(),
                keyType.getTypeSignature(),
                BIGINT.getTypeSignature());

        MethodHandle keyCompareMethodHandle = functionRegistry.resolveOperator(OperatorType.GREATER_THAN, ImmutableList.of(keyType, keyType)).getMethodHandle();
        Comparator comparator = new LongLongComparator(keyCompareMethodHandle);

        AccumulatorStateSerializer stateSerializer = new LongLongStateSerializer(keyType, valueType);
        AccumulatorStateFactory stateFactory = new LongLongStateFactory(comparator);

        List<Type> inputTypes = ImmutableList.of(valueType, keyType, BIGINT);
        Type intermediateType = stateSerializer.getSerializedType();
        Type finalType = new ArrayType(valueType);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, valueType, inputTypes),
                ImmutableList.of(
                        new ParameterMetadata(STATE),
                        new ParameterMetadata(INPUT_CHANNEL, valueType),
                        new ParameterMetadata(INPUT_CHANNEL, keyType),
                        new ParameterMetadata(INPUT_CHANNEL, BIGINT)),
                INPUT_FUNCTION.bindTo(keyCompareMethodHandle),
                null,
                null,
                COMBINE_FUNCTION.bindTo(keyCompareMethodHandle),
                OUTPUT_FUNCTION.bindTo(valueType),
                MaxOrMinByNState.class,
                stateSerializer,
                stateFactory,
                finalType,
                false);

        DynamicClassLoader classLoader = new DynamicClassLoader(MaxByN.class.getClassLoader());
        GenericAccumulatorFactoryBinder factory = new AccumulatorCompiler().generateAccumulatorFactoryBinder(metadata, classLoader);
        InternalAggregationFunction aggregation = new InternalAggregationFunction(NAME, inputTypes, intermediateType, finalType, true, false, factory);

        return new FunctionInfo(signature, getDescription(), aggregation);
    }

    public static void inputLongLong(MethodHandle compare, MaxOrMinByNState state, long value, long key, long n)
    {
        PriorityQueue<LongLongEntry> pq = (PriorityQueue<LongLongEntry>) state.getPriorityQueue();
        if (state.getN() == 0) {
            state.setN(n);
        }
        if (pq.size() >= Ints.checkedCast(n)) {
            boolean compareResult;
            try {
                compareResult = (boolean) compare.invokeExact(key, pq.peek().key);
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(INTERNAL_ERROR, t);
            }
            if (compareResult) {
                pq.remove();
            }
            else {
                return;
            }
        }
        pq.add(new LongLongEntry(key, value));
    }

    private static class LongLongEntry
    {
        final long key;
        final long value;

        public LongLongEntry(long key, long value)
        {
            this.key = key;
            this.value = value;
        }
    }

    public static void combine(MethodHandle compare, MaxOrMinByNState state, MaxOrMinByNState otherState)
    {
        PriorityQueue<LongLongEntry> pq = (PriorityQueue<LongLongEntry>) state.getPriorityQueue();
        PriorityQueue<LongLongEntry> otherPq = (PriorityQueue<LongLongEntry>) otherState.getPriorityQueue();
        if (pq == null) {
            state.setPriorityQueue(otherPq);
            state.setN(otherState.getN());
            return;
        }
        while (!otherPq.isEmpty()) {
            LongLongEntry entry = otherPq.remove();
            if (pq.size() >= Ints.checkedCast(state.getN())) {
                boolean compareResult;
                try {
                    compareResult = (boolean) compare.invokeExact(entry.key, pq.peek().key);
                }
                catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, Error.class);
                    Throwables.propagateIfInstanceOf(t, PrestoException.class);
                    throw new PrestoException(INTERNAL_ERROR, t);
                }
                if (compareResult) {
                    pq.remove();
                }
                else {
                    return;
                }
            }
            pq.add(entry);
        }
    }

    public static void outputLong(Type valueType, MaxOrMinByNState state, BlockBuilder out)
    {
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus());
        PriorityQueue<LongLongEntry> pq = (PriorityQueue<LongLongEntry>) state.getPriorityQueue();
        int resultSize = pq.size();
        long[] result = new long[resultSize];
        for (int i = 0; i < resultSize; i++) {
            LongLongEntry entry = pq.remove();
            result[i] = entry.value;
        }
        for (int i = resultSize - 1; i >= 0; i--) {
            valueType.writeLong(blockBuilder, result[i]);
        }
        VARCHAR.writeSlice(out, buildStructuralSlice(blockBuilder.build()));
    }

    private static class LongLongStateSerializer implements AccumulatorStateSerializer<MaxOrMinByNState>
    {
        private final Type keyType;
        private final Type valueType;

        public LongLongStateSerializer(Type keyType, Type valueType)
        {
            this.keyType = keyType;
            this.valueType = valueType;
        }

        @Override
        public Type getSerializedType()
        {
            return new MapType(keyType, valueType);
        }

        @Override
        public void serialize(MaxOrMinByNState state, BlockBuilder out)
        {
            BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus());
            PriorityQueue<LongLongEntry> pq = (PriorityQueue<LongLongEntry>) state.getPriorityQueue();
            while (!pq.isEmpty()) {
                LongLongEntry entry = pq.remove();
                keyType.writeLong(blockBuilder, entry.key);
                valueType.writeLong(blockBuilder, entry.value);
            }
            VARCHAR.writeSlice(out, buildStructuralSlice(blockBuilder.build()));
        }

        @Override
        public void deserialize(Block block, int index, MaxOrMinByNState state)
        {
            Block mapBlock = readStructuralBlock(VARCHAR.getSlice(block, 0));
            checkState(mapBlock.getPositionCount() % 2 == 0);
            PriorityQueue<LongLongEntry> pq = (PriorityQueue<LongLongEntry>) state.getPriorityQueue();
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                long key = keyType.getLong(mapBlock, i);
                long value = valueType.getLong(mapBlock, i + 1);
                pq.add(new LongLongEntry(key, value));
            }
        }
    }

    public static class LongLongStateFactory
            implements AccumulatorStateFactory<MaxOrMinByNState>
    {
        private final Comparator comparator;

        public LongLongStateFactory(Comparator comparator)
        {
            this.comparator = comparator;
        }

        @Override
        public MaxOrMinByNState createSingleState()
        {
            return new SingleMaxOrMinByNState(comparator);
        }

        @Override
        public Class<? extends MaxOrMinByNState> getSingleStateClass()
        {
            return SingleMaxOrMinByNState.class;
        }

        @Override
        public MaxOrMinByNState createGroupedState()
        {
            return new GroupedMaxOrMinByNState(comparator);
        }

        @Override
        public Class<? extends MaxOrMinByNState> getGroupedStateClass()
        {
            return GroupedMaxOrMinByNState.class;
        }

        public static class GroupedMaxOrMinByNState
                extends AbstractGroupedAccumulatorState
                implements MaxOrMinByNState
        {
            private final ObjectBigArray<PriorityQueue> objects = new ObjectBigArray<PriorityQueue>();
            private final Comparator comparator;
            private final LongBigArray longs = new LongBigArray();

            public GroupedMaxOrMinByNState(Comparator comparator)
            {
                this.comparator = comparator;
            }

            @Override
            public void ensureCapacity(long size)
            {
                objects.ensureCapacity(size);
                for (int i = 0; i < size; i++) {
                    objects.set(i, new PriorityQueue(comparator));
                }
                longs.ensureCapacity(size);
            }

            @Override
            public long getEstimatedSize()
            {
                long size = 0;
                //TODO: size?
                size += objects.sizeOf();
                return size;
            }

            @Override
            public PriorityQueue getPriorityQueue()
            {
                return objects.get(getGroupId());
            }

            @Override
            public void setPriorityQueue(PriorityQueue object)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getN()
            {
                return longs.get(getGroupId());
            }

            @Override
            public void setN(long n)
            {
                longs.set(getGroupId(), n);
            }
        }

        public static class SingleMaxOrMinByNState
                implements MaxOrMinByNState
        {
            private final PriorityQueue object;
            private long n;

            public SingleMaxOrMinByNState(Comparator comparator)
            {
                object = new PriorityQueue(comparator);
            }

            @Override
            public long getEstimatedSize()
            {
                //TODO: size?
                return 1;
            }

            @Override
            public PriorityQueue getPriorityQueue()
            {
                return object;
            }

            @Override
            public void setPriorityQueue(PriorityQueue object)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getN()
            {
                return n;
            }

            @Override
            public void setN(long n)
            {
                this.n = n;
            }
        }
    }

    public static class LongLongComparator
        implements Comparator<LongLongEntry>
    {
        MethodHandle keyCompare;

        public LongLongComparator(MethodHandle keyCompare)
        {
            this.keyCompare = keyCompare;
        }

        @Override
        public int compare(LongLongEntry o1, LongLongEntry o2)
        {
            try {
                return (boolean) keyCompare.invokeExact(o1.key, o2.key) ? 1 : -1;
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }
    }
}
