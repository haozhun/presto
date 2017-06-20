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
package com.facebook.presto.operator;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class HashBuilderOperator
        implements Operator
{
    public static class HashBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> types;
        private final DriverGroupEntityManager.LookupSourceFactoryManager lookupSourceFactoryManager;
        private final List<Integer> outputChannels;
        private final List<Integer> hashChannels;
        private final Optional<Integer> preComputedHashChannel;
        private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
        private final PagesIndex.Factory pagesIndexFactory;

        private final int expectedPositions;

        private final DriverGroupEntityManager<AtomicInteger> partitionIndexManager = new DriverGroupEntityManager<>(driverGroupId -> new AtomicInteger(0));

        private boolean closed;

        public HashBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> types,
                DriverGroupEntityManager.LookupSourceFactoryManager lookupSourceFactory,
                List<Integer> outputChannels,
                List<Integer> hashChannels,
                Optional<Integer> preComputedHashChannel,
                Optional<JoinFilterFunctionFactory> filterFunctionFactory,
                int expectedPositions,
                PagesIndex.Factory pagesIndexFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.types = requireNonNull(types, "types is null");

            this.lookupSourceFactoryManager = requireNonNull(lookupSourceFactory, "lookupSourceFactoryManager");

            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
            this.preComputedHashChannel = requireNonNull(preComputedHashChannel, "preComputedHashChannel is null");
            this.filterFunctionFactory = requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");
            this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");

            this.expectedPositions = expectedPositions;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashBuilderOperator.class.getSimpleName());

            PartitionedLookupSourceFactory lookupSourceFactory = (PartitionedLookupSourceFactory) this.lookupSourceFactoryManager.forDriverGroup(driverContext.getDriverGroup());
            int partitionIndex = partitionIndexManager.forDriverGroup(driverContext.getDriverGroup()).getAndIncrement();
            TaskId taskId = driverContext.getTaskId();
            if (taskId.getStageId().getId() == 1 && taskId.getId() == 0) {
                System.out.println(String.format("Build SourceFactory %s %s", lookupSourceFactory, partitionIndex));
            }
            verify(partitionIndex < lookupSourceFactory.getPartitionCount());
            return new HashBuilderOperator(
                    operatorContext,
                    lookupSourceFactory,
                    partitionIndex,
                    outputChannels,
                    hashChannels,
                    preComputedHashChannel,
                    filterFunctionFactory,
                    expectedPositions,
                    pagesIndexFactory);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Parallel hash build can not be duplicated");
        }
    }

    private final OperatorContext operatorContext;
    private final PartitionedLookupSourceFactory lookupSourceFactory;
    private final int partitionIndex;

    private final List<Integer> outputChannels;
    private final List<Integer> hashChannels;
    private final Optional<Integer> preComputedHashChannel;
    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;

    private final PagesIndex index;

    private boolean finishing;
    private final HashCollisionsCounter hashCollisionsCounter;

    public HashBuilderOperator(
            OperatorContext operatorContext,
            PartitionedLookupSourceFactory lookupSourceFactory,
            int partitionIndex,
            List<Integer> outputChannels,
            List<Integer> hashChannels,
            Optional<Integer> preComputedHashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory)
    {
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");

        this.operatorContext = operatorContext;
        this.partitionIndex = partitionIndex;
        this.filterFunctionFactory = filterFunctionFactory;

        this.index = pagesIndexFactory.newPagesIndex(lookupSourceFactory.getTypes(), expectedPositions);
        this.lookupSourceFactory = lookupSourceFactory;

        this.outputChannels = outputChannels;
        this.hashChannels = hashChannels;
        this.preComputedHashChannel = preComputedHashChannel;

        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return lookupSourceFactory.getTypes();
    }

    @Override
    public void finish()
    {
        if (finishing) {
            return;
        }
        finishing = true;

        System.out.println(String.format(
                "HJIN5 Finishing Operator: Task %s.%s Pipeline %s DriverGroup %s   %s",
                operatorContext.getDriverContext().getTaskId().getStageId().getId(),
                operatorContext.getDriverContext().getTaskId().getId(),
                operatorContext.getDriverContext().getPipelineContext().getPipelineId(),
                operatorContext.getDriverContext().getDriverGroup(),
                this.getOperatorContext()));

        LookupSourceSupplier partition = index.createLookupSourceSupplier(operatorContext.getSession(), hashChannels, preComputedHashChannel, filterFunctionFactory, Optional.of(outputChannels));
        lookupSourceFactory.setPartitionLookupSourceSupplier(partitionIndex, partition);

        operatorContext.setMemoryReservation(partition.get().getInMemorySizeInBytes());
        hashCollisionsCounter.recordHashCollision(partition.getHashCollisions(), partition.getExpectedHashCollisions());
    }

    @Override
    public boolean isFinished()
    {
        return finishing && lookupSourceFactory.isDestroyed().isDone();
    }

    @Override
    public boolean needsInput()
    {
        return !finishing;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!finishing) {
            return NOT_BLOCKED;
        }
        return lookupSourceFactory.isDestroyed();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!isFinished(), "Operator is already finished");

        index.addPage(page);
        if (!operatorContext.trySetMemoryReservation(index.getEstimatedSize().toBytes())) {
            index.compact();
        }
        operatorContext.setMemoryReservation(index.getEstimatedSize().toBytes());
        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
