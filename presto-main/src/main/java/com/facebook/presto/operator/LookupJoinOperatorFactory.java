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

import com.facebook.presto.execution.DriverGroupId;
import com.facebook.presto.operator.DriverGroupEntityManager.LookupSourceFactoryManager;
import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.operator.LookupOuterOperator.LookupOuterOperatorFactory;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.operator.LookupJoinOperators.JoinType.INNER;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class LookupJoinOperatorFactory
        implements JoinOperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final List<Type> probeOutputTypes;
    private final List<Type> buildOutputTypes;
    private final JoinType joinType;
    private final LookupSourceFactoryManager lookupSourceFactoryManager;
    private final JoinProbeFactory joinProbeFactory;
    private final Optional<OperatorFactory> outerOperatorFactory;
    private final PerDriverGroupDataManager perDriverGroupDataManager;
    private boolean closed;

    private final FreezeOnReadCounter factoryCount;

    public LookupJoinOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            LookupSourceFactoryManager lookupSourceFactoryManager,
            List<Type> probeOutputTypes,
            List<Type> buildOutputTypes,
            JoinType joinType,
            JoinProbeFactory joinProbeFactory)
    {
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.lookupSourceFactoryManager = requireNonNull(lookupSourceFactoryManager, "lookupSourceFactoryManager is null");
        this.probeOutputTypes = ImmutableList.copyOf(requireNonNull(probeOutputTypes, "probeOutputTypes is null"));
        this.buildOutputTypes = ImmutableList.copyOf(requireNonNull(buildOutputTypes, "buildOutputTypes is null"));
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.factoryCount = new FreezeOnReadCounter();
        this.factoryCount.increment();

        this.perDriverGroupDataManager = new PerDriverGroupDataManager(joinType, factoryCount, lookupSourceFactoryManager);

        if (joinType == INNER || joinType == PROBE_OUTER) {
            this.outerOperatorFactory = Optional.empty();
        }
        else {
            this.outerOperatorFactory = Optional.of(new LookupOuterOperatorFactory(
                    operatorId,
                    planNodeId,
                    perDriverGroupDataManager::getOuterPositionsFuture,
                    probeOutputTypes,
                    buildOutputTypes,
                    perDriverGroupDataManager::getLookupSourceFactoryUsersCount,
                    perDriverGroupDataManager::noMoreGroupsForLookupOuterOperator));
        }
    }

    private LookupJoinOperatorFactory(LookupJoinOperatorFactory other)
    {
        requireNonNull(other, "other is null");
        operatorId = other.operatorId;
        planNodeId = other.planNodeId;
        probeOutputTypes = other.probeOutputTypes;
        buildOutputTypes = other.buildOutputTypes;
        joinType = other.joinType;
        lookupSourceFactoryManager = other.lookupSourceFactoryManager;
        joinProbeFactory = other.joinProbeFactory;
        perDriverGroupDataManager = other.perDriverGroupDataManager;
        outerOperatorFactory = other.outerOperatorFactory;
        factoryCount = other.factoryCount;
    }

    public int getOperatorId()
    {
        return operatorId;
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.<Type>builder()
                .addAll(probeOutputTypes)
                .addAll(buildOutputTypes)
                .build();
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");
        LookupSourceFactory lookupSourceFactory = lookupSourceFactoryManager.forDriverGroup(driverContext.getDriverGroup());
        ReferenceCount probeReferenceCount = perDriverGroupDataManager.getProbeReferenceCount(driverContext.getDriverGroup());

        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LookupJoinOperator.class.getSimpleName());

        lookupSourceFactory.setTaskContext(driverContext.getPipelineContext().getTaskContext());

        probeReferenceCount.retain();
        return new LookupJoinOperator(
                operatorContext,
                getTypes(),
                joinType,
                lookupSourceFactory.createLookupSource(),
                joinProbeFactory,
                probeReferenceCount::release);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        // TODO! make it possible to release hash build when a driver group completes (instead of when everything finishes)
        perDriverGroupDataManager.noMoreGroupsForLookupJoinOperator();
    }

    @Override
    public void noMoreOperator(DriverGroupId driverGroupId)
    {
        perDriverGroupDataManager.getProbeReferenceCount(driverGroupId).release();
    }

    @Override
    public OperatorFactory duplicate()
    {
        return new LookupJoinOperatorFactory(this);
    }

    @Override
    public Optional<OperatorFactory> createOuterOperatorFactory()
    {
        return outerOperatorFactory;
    }

    public static class PerDriverGroupDataManager
    {
        private final JoinType joinType;
        private final FreezeOnReadCounter factoryCount;
        private final LookupSourceFactoryManager lookupSourceFactoryManager;

        // In addition to common reasons for creating 1 map instead of 3, an additional reason applies here.
        // The initialization for all/some of these values have side effects. As a result, making sure all 3
        // at the same time simplifies reasoning about correctness.
        private final Map<DriverGroupId, PerDriverGroupData> map = new ConcurrentHashMap<>();
        private final AtomicBoolean noMoreGroupsForLookupJoin = new AtomicBoolean();
        private final AtomicBoolean noMoreGroupsForLookupOuter = new AtomicBoolean();

        public PerDriverGroupDataManager(JoinType joinType, FreezeOnReadCounter factoryCount, LookupSourceFactoryManager lookupSourceFactoryManager)
        {
            this.joinType = joinType;
            this.factoryCount = factoryCount;
            this.lookupSourceFactoryManager = lookupSourceFactoryManager;
        }

        private ReferenceCount getProbeReferenceCount(DriverGroupId driverGroupId)
        {
            return data(driverGroupId).getProbeReferenceCount();
        }

        public ReferenceCount getLookupSourceFactoryUsersCount(DriverGroupId driverGroupId)
        {
            return data(driverGroupId).getLookupSourceFactoryUsersCount();
        }

        public ListenableFuture<OuterPositionIterator> getOuterPositionsFuture(DriverGroupId driverGroupId)
        {
            return data(driverGroupId).getOuterPositionsFuture();
        }

        private void noMoreGroupsForLookupJoinOperator()
        {
            //TODO! this should be no-op?
            /*
            if (noMoreGroupsForLookupJoin.compareAndSet(false, true)) {
                for (PerDriverGroupData perDriverGroupData : map.values()) {
                    boolean done = perDriverGroupData.getProbeReferenceCount().getFreeFuture().isDone();
                    if (!done) {
                        System.out.println("X");
                    }
                    checkState(done);
                    //perDriverGroupData.getProbeReferenceCount().release();
                }
            }
            */
        }

        private void noMoreGroupsForLookupOuterOperator()
        {
            //TODO! this should be no-op?
            /*
            if (noMoreGroupsForLookupOuter.compareAndSet(false, true)) {
                for (PerDriverGroupData perDriverGroupData : map.values()) {
                    checkState(perDriverGroupData.getProbeReferenceCount().getFreeFuture().isDone());
                    //perDriverGroupData.getLookupSourceFactoryUsersCount().release();
                }
            }
            */
        }

        private PerDriverGroupData data(DriverGroupId driverGroupId)
        {
            checkState(!noMoreGroupsForLookupJoin.get() && !noMoreGroupsForLookupOuter.get());
            return map.computeIfAbsent(
                    driverGroupId,
                    id -> new PerDriverGroupData(id, joinType, factoryCount.get(), lookupSourceFactoryManager.forDriverGroup(id)));
        }
    }

    public static class PerDriverGroupData
    {
        private final DriverGroupId driverGroupId;
        private final ReferenceCount probeReferenceCount;
        private final ReferenceCount lookupSourceFactoryUsersCount;
        private final ListenableFuture<OuterPositionIterator> outerPositionsFuture;

        public PerDriverGroupData(DriverGroupId driverGroupId, JoinType joinType, int factoryCount, LookupSourceFactory lookupSourceFactory)
        {
            this.driverGroupId = driverGroupId;

            // When all probe and build-outer operators finish, destroy the lookup source (freeing the memory)
            // Whole probe side is counted as 1 in lookupSourceFactoryUsersCount
            lookupSourceFactoryUsersCount = new ReferenceCount("user+" + driverGroupId.toString(), 1);
            lookupSourceFactoryUsersCount.getFreeFuture().addListener(lookupSourceFactory::destroy, directExecutor());

            probeReferenceCount = new ReferenceCount("probe+" + driverGroupId.toString(), factoryCount);
            probeReferenceCount.getFreeFuture().addListener(lookupSourceFactoryUsersCount::release, directExecutor());

            if (joinType == INNER || joinType == PROBE_OUTER) {
                outerPositionsFuture = null;
            }
            else {
                // increment the user count by 1 to account for the build-outer factory
                lookupSourceFactoryUsersCount.retain();

                // when all join operators finish (and lookup source is ready), set the outer position future to start the outer operator
                ListenableFuture<LookupSource> lookupSourceAfterProbeFinished = transformAsync(probeReferenceCount.getFreeFuture(), ignored -> lookupSourceFactory.createLookupSource());
                outerPositionsFuture = transform(lookupSourceAfterProbeFinished, lookupSource -> {
                    lookupSource.close();
                    return lookupSourceFactory.getOuterPositionIterator();
                });
            }
        }

        public ReferenceCount getProbeReferenceCount()
        {
            return probeReferenceCount;
        }

        public ReferenceCount getLookupSourceFactoryUsersCount()
        {
            return lookupSourceFactoryUsersCount;
        }

        public ListenableFuture<OuterPositionIterator> getOuterPositionsFuture()
        {
            return outerPositionsFuture;
        }
    }

    public static class FreezeOnReadCounter
    {
        private int count;
        private boolean freezed;

        public synchronized void increment()
        {
            checkState(!freezed, "Counter has been read");
            count++;
        }

        public synchronized int get()
        {
            freezed = true;
            return count;
        }
    }
}
