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
package com.facebook.presto.execution;

import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.buffer.BufferState;
import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.execution.executor.TaskExecutor;
import com.facebook.presto.execution.executor.TaskHandle;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.DriverStats;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.exchange.LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.ExecutionFlowStrategy;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.units.Duration;
import org.eclipse.jetty.util.ConcurrentHashSet;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.getInitialSplitsPerNode;
import static com.facebook.presto.SystemSessionProperties.getSplitConcurrencyAdjustmentInterval;
import static com.facebook.presto.execution.SqlTaskExecution.SplitsState.DONE;
import static com.facebook.presto.execution.SqlTaskExecution.SplitsState.INITIAL;
import static com.facebook.presto.execution.SqlTaskExecution.SplitsState.NO_MORE_SPLITS;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class SqlTaskExecution
{
    private final TaskId taskId;
    private final TaskStateMachine taskStateMachine;
    private final TaskContext taskContext;
    private final OutputBuffer outputBuffer;

    private final TaskHandle taskHandle;
    private final TaskExecutor taskExecutor;

    private final Executor notificationExecutor;

    private final QueryMonitor queryMonitor;

    private final List<WeakReference<Driver>> drivers = new CopyOnWriteArrayList<>();

    // guarded for update only
    @GuardedBy("this")
    private final ConcurrentMap<PlanNodeId, TaskSource> unpartitionedSources = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private long maxAcknowledgedSplit = Long.MIN_VALUE;

    private final Map<PlanNodeId, DriverSplitRunnerFactory> partitionedDriverFactories;

    private final List<PlanNodeId> sourceStartOrder;

    @GuardedBy("this")
    private final SchedulingDriverGroups schedulingDriverGroups;

    @GuardedBy("this")
    private final Map<PlanNodeId, SplitsForPlanNode> pendingSplitsMap;

    @GuardedBy("this")
    private final Set<DriverGroupId> completedDriverGroups = new ConcurrentHashSet<>();

    private final List<DriverSplitRunnerFactory> unpartitionedDriverFactories;

    private final Status status;

    public static SqlTaskExecution createSqlTaskExecution(
            TaskStateMachine taskStateMachine,
            TaskContext taskContext,
            OutputBuffer outputBuffer,
            PlanFragment fragment,
            List<TaskSource> sources,
            LocalExecutionPlanner planner,
            TaskExecutor taskExecutor,
            Executor notificationExecutor,
            QueryMonitor queryMonitor)
    {
        SqlTaskExecution task = new SqlTaskExecution(
                taskStateMachine,
                taskContext,
                outputBuffer,
                fragment,
                planner,
                taskExecutor,
                queryMonitor,
                notificationExecutor
        );

        try (SetThreadName ignored = new SetThreadName("Task-%s", task.getTaskId())) {
            task.start();
            task.addSources(sources);
            return task;
        }
    }

    private SqlTaskExecution(
            TaskStateMachine taskStateMachine,
            TaskContext taskContext,
            OutputBuffer outputBuffer,
            PlanFragment fragment,
            LocalExecutionPlanner planner,
            TaskExecutor taskExecutor,
            QueryMonitor queryMonitor,
            Executor notificationExecutor)
    {
        this.taskStateMachine = requireNonNull(taskStateMachine, "taskStateMachine is null");
        this.taskId = taskStateMachine.getTaskId();
        this.taskContext = requireNonNull(taskContext, "taskContext is null");
        this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");

        this.taskExecutor = requireNonNull(taskExecutor, "driverExecutor is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");

        this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");

        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            List<DriverFactory> driverFactories;
            try {
                LocalExecutionPlan localExecutionPlan = planner.plan(
                        taskContext,
                        fragment.getRoot(),
                        fragment.getSymbols(),
                        fragment.getPartitioningScheme(),
                        outputBuffer);
                driverFactories = localExecutionPlan.getDriverFactories();

                if (taskId.getId() == 0) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("Pipelines for Stage ").append(taskId.getStageId().getId()).append(":\n");
                    for (DriverFactory driverFactory : driverFactories) {
                        sb.append("  Pipeline ").append(driverFactory.getPipelineId()).append(":");
                        sb.append("  ").append(driverFactory.getExecutionFlowStrategy());
                        sb.append('\n');
                        for (OperatorFactory operatorFactory : driverFactory.getOperatorFactories()) {
                            sb.append("    ").append(operatorFactory.getClass().getTypeName()).append('\n');
                        }
                    }
                    System.out.println(sb.toString());
                }
            }
            catch (Throwable e) {
                // planning failed
                taskStateMachine.failed(e);
                throw Throwables.propagate(e);
            }

            // index driver factories
            ImmutableMap.Builder<PlanNodeId, DriverSplitRunnerFactory> partitionedDriverFactories = ImmutableMap.builder();
            ImmutableMap.Builder<PlanNodeId, SplitsForPlanNode> pendingSplitsMap = ImmutableMap.builder();
            ImmutableList.Builder<DriverSplitRunnerFactory> unpartitionedDriverFactories = ImmutableList.builder();
            ImmutableList.Builder<Integer> pipelineId = ImmutableList.builder();
            for (DriverFactory driverFactory : driverFactories) {
                Optional<PlanNodeId> sourceId = driverFactory.getSourceId();
                pipelineId.add(driverFactory.getPipelineId());
                if (sourceId.isPresent() && fragment.isPartitionedSources(sourceId.get())) {
                    partitionedDriverFactories.put(sourceId.get(), new DriverSplitRunnerFactory(driverFactory));
                    pendingSplitsMap.put(sourceId.get(), new SplitsForPlanNode());
                }
                else {
                    OperatorFactory firstOperatorFactory = driverFactory.getOperatorFactories().get(0);
                    if (driverFactory.getExecutionFlowStrategy() == ExecutionFlowStrategy.PER_BUCKET) {
                        checkArgument(firstOperatorFactory instanceof LocalExchangeSourceOperatorFactory);
                    }
                    unpartitionedDriverFactories.add(new DriverSplitRunnerFactory(driverFactory));
                }
            }
            this.partitionedDriverFactories = partitionedDriverFactories.build();
            this.unpartitionedDriverFactories = unpartitionedDriverFactories.build();
            this.pendingSplitsMap = pendingSplitsMap.build();
            this.sourceStartOrder = ImmutableList.copyOf(fragment.getPartitionedSources());
            this.schedulingDriverGroups = new SchedulingDriverGroups(this.partitionedDriverFactories, this.sourceStartOrder);
            this.status = new Status(pipelineId.build());
            System.out.println(String.format("HJIN6: taskId %s.%s partitioned %s unpartitioned %s pendingSplitsMap %s sourceStartOrder %s", taskId.getStageId().getId(), taskId.getId(), this.partitionedDriverFactories.size(), this.unpartitionedDriverFactories.size(), this.pendingSplitsMap.size(), this.sourceStartOrder));

            checkArgument(this.partitionedDriverFactories.keySet().equals(ImmutableSet.copyOf(fragment.getPartitionedSources())),
                    "Fragment us partitioned, but all partitioned drivers were not found");

            // don't register the task if it is already completed (most likely failed during planning above)
            if (!taskStateMachine.getState().isDone()) {
                taskHandle = taskExecutor.addTask(taskId, outputBuffer::getUtilization, getInitialSplitsPerNode(taskContext.getSession()), getSplitConcurrencyAdjustmentInterval(taskContext.getSession()));
                taskStateMachine.addStateChangeListener(new RemoveTaskHandleWhenDone(taskExecutor, taskHandle));
                taskStateMachine.addStateChangeListener(state -> {
                    if (state.isDone()) {
                        for (DriverFactory factory : driverFactories) {
                            factory.close();
                        }
                    }
                });
            }
            else {
                taskHandle = null;
            }

            outputBuffer.addStateChangeListener(new CheckTaskCompletionOnBufferFinish(SqlTaskExecution.this));
        }
    }

    //
    // This code starts registers a callback with access to this class, and this
    // call back is access from another thread, so this code can not be placed in the constructor
    private void start()
    {
        // start unpartitioned drivers
        List<DriverSplitRunner> runners = new ArrayList<>();
        for (DriverSplitRunnerFactory driverFactory : unpartitionedDriverFactories) {
            if (driverFactory.getExecutionFlowStrategy() == ExecutionFlowStrategy.PER_BUCKET) {
                continue;
            }
            for (int i = 0; i < driverFactory.getDriverInstances().orElse(1); i++) {
                runners.add(driverFactory.createDriverRunner(null, false, DriverGroupId.empty()));
            }
        }
        enqueueDrivers(true, runners);
        for (DriverSplitRunnerFactory driverFactory : unpartitionedDriverFactories) {
            if (driverFactory.getExecutionFlowStrategy() == ExecutionFlowStrategy.PER_BUCKET) {
                continue;
            }
            driverFactory.setNoMoreSplits();
        }
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    public TaskContext getTaskContext()
    {
        return taskContext;
    }

    public void addSources(List<TaskSource> sources)
    {
        requireNonNull(sources, "sources is null");
        checkState(!Thread.holdsLock(this), "Can not add sources while holding a lock on the %s", getClass().getSimpleName());

        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            // update our record of sources and schedule drivers for new partitioned splits
            Map<PlanNodeId, TaskSource> updatedUnpartitionedSources = updateSources(sources);

            // tell existing drivers about the new splits; it is safe to update drivers
            // multiple times and out of order because sources contain full record of
            // the unpartitioned splits
            // INTERESTING
            for (TaskSource source : updatedUnpartitionedSources.values()) {
                // tell all the existing drivers this source is finished
                for (WeakReference<Driver> driverReference : drivers) {
                    Driver driver = driverReference.get();
                    // the driver can be GCed due to a failure or a limit
                    if (driver != null) {
                        driver.updateSource(source);
                    }
                    else {
                        // remove the weak reference from the list to avoid a memory leak
                        // NOTE: this is a concurrent safe operation on a CopyOnWriteArrayList
                        drivers.remove(driverReference);
                    }
                }
            }
            // END INTERESTING

            // we may have transitioned to no more splits, so check for completion
            checkTaskCompletion();
        }
    }

    private synchronized Map<PlanNodeId, TaskSource> updateSources(List<TaskSource> sources)
    {
        Map<PlanNodeId, TaskSource> updatedUnpartitionedSources = new HashMap<>();

        // first remove any split that was already acknowledged
        long currentMaxAcknowledgedSplit = this.maxAcknowledgedSplit;
        sources = sources.stream()
                .map(source -> new TaskSource(
                        source.getPlanNodeId(),
                        source.getSplits().stream()
                                .filter(scheduledSplit -> scheduledSplit.getSequenceId() > currentMaxAcknowledgedSplit)
                                .collect(Collectors.toSet()),
                        source.getNoMoreSplitsForDriverGroup(), // TODO! prune this?
                        source.isNoMoreSplits()))
                .collect(toList());

        // update task with new sources
        for (TaskSource source : sources) {
            if (partitionedDriverFactories.containsKey(source.getPlanNodeId())) {
                schedulePartitionedSource(source);
            }
            else {
                scheduleUnpartitionedSource(source, updatedUnpartitionedSources);
            }
        }

        // update maxAcknowledgedSplit
        maxAcknowledgedSplit = sources.stream()
                .flatMap(source -> source.getSplits().stream())
                .mapToLong(ScheduledSplit::getSequenceId)
                .max()
                .orElse(maxAcknowledgedSplit);
        return updatedUnpartitionedSources;
    }

    @GuardedBy("this")
    private void mergeIntoPendingSplits(PlanNodeId planNodeId, Set<ScheduledSplit> scheduledSplits, Set<DriverGroupId> noMoreSplitsForDriverGroup, boolean noMoreSplits)
    {
        checkHoldsLock();

        DriverSplitRunnerFactory partitionedDriverFactory = partitionedDriverFactories.get(planNodeId);
        SplitsForPlanNode pendingSplitsForPlanNode = pendingSplitsMap.get(planNodeId);

        for (ScheduledSplit scheduledSplit : scheduledSplits) {
            DriverGroupId driverGroupId;
            if (partitionedDriverFactory.getExecutionFlowStrategy() == ExecutionFlowStrategy.PER_BUCKET) {
                driverGroupId = DriverGroupId.of(scheduledSplit.getSplit().getConnectorSplit().getDriverGroupId());
            }
            else {
                driverGroupId = DriverGroupId.empty();
            }
            pendingSplitsForPlanNode.getDriverGroup(driverGroupId).addSplit(scheduledSplit);
            schedulingDriverGroups.addDriverGroupIfAbsent(driverGroupId);
        }
        for (DriverGroupId driverGroupWithNoMoreSplits : noMoreSplitsForDriverGroup) {
            SplitsForDriverGroup pendingSplitsForDriverGroup = pendingSplitsForPlanNode.getDriverGroup(driverGroupWithNoMoreSplits);
            if (pendingSplitsForDriverGroup.getState() == INITIAL) {
                pendingSplitsForDriverGroup.setState(NO_MORE_SPLITS);
            }
        }
        if (noMoreSplits) {
            pendingSplitsForPlanNode.setNoMoreSplits(true);
        }
    }

    // Splits for a particular plan node (all driver groups)
    class SplitsForPlanNode
    {
        private final Map<DriverGroupId, SplitsForDriverGroup> map = new HashMap<>();
        private boolean noMoreSplits;

        public SplitsForDriverGroup getDriverGroup(DriverGroupId driverGroupId)
        {
            return map.computeIfAbsent(driverGroupId, ignored -> new SplitsForDriverGroup());
        }

        public boolean isNoMoreSplits()
        {
            return noMoreSplits;
        }

        public void setNoMoreSplits(boolean noMoreSplits)
        {
            this.noMoreSplits = noMoreSplits;
        }
    }

    // Splits for a particular plan node and driver group combination
    class SplitsForDriverGroup
    {
        private Set<ScheduledSplit> splits = new HashSet<>();
        private SplitsState state = INITIAL;

        public SplitsState getState()
        {
            return state;
        }

        public void setState(SplitsState state)
        {
            this.state = requireNonNull(state, "state is null");
        }

        public void addSplit(ScheduledSplit scheduledSplit)
        {
            splits.add(scheduledSplit);
        }

        public Set<ScheduledSplit> removeAllSplits()
        {
            Set<ScheduledSplit> result = splits;
            splits = new HashSet<>();
            return result;
        }
    }

    enum SplitsState
    {
        INITIAL,
        NO_MORE_SPLITS,
        DONE
    }

    private /*static*/ class SchedulingDriverGroups
            implements Iterable<SchedulingDriverGroup>
    {
        private final Map<PlanNodeId, DriverSplitRunnerFactory> partitionedDriverFactories;
        private final List<PlanNodeId> sourceStartOrder;

        private final Map<DriverGroupId, SchedulingDriverGroup> driverGroups = new HashMap<>();
        private final Map<PlanNodeId, AtomicInteger> finishedDriverGroupCount;
        private int driverGroupCount;
        private boolean noMoreDriverGroups;

        public SchedulingDriverGroups(Map<PlanNodeId, DriverSplitRunnerFactory> partitionedDriverFactories, List<PlanNodeId> sourceStartOrder)
        {
            this.partitionedDriverFactories = partitionedDriverFactories;
            this.sourceStartOrder = sourceStartOrder;

            this.finishedDriverGroupCount = sourceStartOrder.stream()
                    .collect(Collectors.toMap(Function.identity(), ignored -> new AtomicInteger()));
        }

        public void noMoreDriverGroups()
        {
            if (noMoreDriverGroups) {
                return;
            }

            System.out.println(String.format("HJIN5 Finishing SchedulingDriverGroups: Task %s.%s", taskContext.getTaskId().getStageId().getId(), taskContext.getTaskId().getId()));
            noMoreDriverGroups = true;

            for (Entry<PlanNodeId, AtomicInteger> entry : finishedDriverGroupCount.entrySet()) {
                PlanNodeId planNodeId = entry.getKey();
                AtomicInteger count = entry.getValue();
                if (count.get() == driverGroupCount) {
                    partitionedDriverFactories.get(planNodeId).setNoMoreSplits();
                }
            }
        }

        public void addDriverGroupIfAbsent(DriverGroupId driverGroupId)
        {
            if (driverGroups.containsKey(driverGroupId)) {
                return;
            }
            driverGroupCount++;
            driverGroups.put(driverGroupId, new SchedulingDriverGroup(
                    driverGroupId,
                    sourceStartOrder,
                    planNodeId -> {
                        AtomicInteger count = finishedDriverGroupCount.get(planNodeId);
                        if (count.incrementAndGet() == driverGroupCount && noMoreDriverGroups) {
                            System.out.println(String.format("HJIN5 Calling PartitionedDriverFactory.Finish: Task %s.%s PlanNode %s", taskContext.getTaskId().getStageId().getId(), taskContext.getTaskId().getId(), planNodeId));
                            partitionedDriverFactories.get(planNodeId).setNoMoreSplits();
                        }
                    }));
        }

        @GuardedBy("this") // SqlTaskExecution.this
        public void removeCompletelyScheduled()
        {
            Iterator<SchedulingDriverGroup> iterator = driverGroups.values().iterator();
            while (iterator.hasNext()) {
                SchedulingDriverGroup schedulingDriverGroup = iterator.next();
                if (schedulingDriverGroup.isDone()) {
                    iterator.remove();
                }
            }
        }

        @Override
        public Iterator<SchedulingDriverGroup> iterator()
        {
            return driverGroups.values().iterator();
        }
    }

    private static class SchedulingDriverGroup
    {
        private final DriverGroupId driverGroupId;
        private final List<PlanNodeId> planNodeSchedulingOrder;
        private final Consumer<PlanNodeId> completeListener;
        private int schedulingPlanNodeOrdinal;
        private boolean unpartitionedDriversScheduled;

        public SchedulingDriverGroup(DriverGroupId driverGroupId, List<PlanNodeId> planNodeSchedulingOrder, Consumer<PlanNodeId> completeListener)
        {
            this.driverGroupId = driverGroupId;
            this.planNodeSchedulingOrder = planNodeSchedulingOrder;
            this.completeListener = completeListener;
        }

        public DriverGroupId getDriverGroupId()
        {
            return driverGroupId;
        }

        public PlanNodeId getSchedulingPlanNode()
        {
            checkState(!isDone());
            return planNodeSchedulingOrder.get(schedulingPlanNodeOrdinal);
        }

        public void nextPlanNode()
        {
            checkState(!isDone());
            completeListener.accept(getSchedulingPlanNode());
            schedulingPlanNodeOrdinal++;
        }

        public boolean isDone()
        {
            return schedulingPlanNodeOrdinal >= planNodeSchedulingOrder.size();
        }

        public boolean shouldScheduleUnpartitionedDrivers()
        {
            if (unpartitionedDriversScheduled) {
                return false;
            }
            unpartitionedDriversScheduled = true;
            return true;
        }
    }

    private synchronized void schedulePartitionedSource(TaskSource sourceUpdate)
    {
        mergeIntoPendingSplits(sourceUpdate.getPlanNodeId(), sourceUpdate.getSplits(), sourceUpdate.getNoMoreSplitsForDriverGroup(), sourceUpdate.isNoMoreSplits());

        for (SchedulingDriverGroup schedulingDriverGroup : schedulingDriverGroups) {
            DriverGroupId driverGroupId = schedulingDriverGroup.getDriverGroupId();

            // Schedule the currently scheduling plan node for each driver group.

            // For a particular driver group, if the currently scheduling plan node does not match sourceUpdate.getPlanNodeId(), this would be a no-op,
            // unless its preceding plan node(s) happens to finish scheduling due to the new splits in sourceUpdate, directly or by cascading.
            while (true) {
                PlanNodeId schedulingPlanNode = schedulingDriverGroup.getSchedulingPlanNode();
                DriverSplitRunnerFactory partitionedDriverFactory = partitionedDriverFactories.get(schedulingPlanNode);
                SplitsForPlanNode splitsForPlanNode = pendingSplitsMap.get(schedulingPlanNode);
                SplitsForDriverGroup splitsForDriverGroup = splitsForPlanNode.getDriverGroup(driverGroupId);

                System.out.println(String.format("HJIN8: processing TaskSource update: Task %s.%s DriverGroup %s Pipeline %s PlanNode %s", taskId.getStageId().getId(), taskId.getId(), driverGroupId, partitionedDriverFactory.pipelineContext.getPipelineId(), schedulingPlanNode));

                if (schedulingDriverGroup.shouldScheduleUnpartitionedDrivers()) {
                    scheduleUnpartitionedFor(driverGroupId);
                }
                ImmutableList.Builder<DriverSplitRunner> runners = ImmutableList.builder();
                for (ScheduledSplit scheduledSplit : splitsForDriverGroup.removeAllSplits()) {
                    // create a new driver for the split
                    runners.add(partitionedDriverFactory.createDriverRunner(scheduledSplit, true, driverGroupId));
                }
                enqueueDrivers(false, runners.build());
                if (!splitsForPlanNode.isNoMoreSplits() && splitsForDriverGroup.getState() != NO_MORE_SPLITS) {
                    break;
                }
                if (splitsForPlanNode.isNoMoreSplits() && splitsForDriverGroup.getState() == NO_MORE_SPLITS) {
                    // TODO! should this ever happen?
                }
                partitionedDriverFactory.noMoreSplitsForDriver(ImmutableList.of(driverGroupId));
                splitsForDriverGroup.setState(DONE);

                schedulingDriverGroup.nextPlanNode();
                if (schedulingDriverGroup.isDone()) {
                    break;
                }
            }
        }
        schedulingDriverGroups.removeCompletelyScheduled();

        // TODO! This seems wrong? This indicates that there are no more driver groups for a particular plan node, not globally
        // When all plan nodes in a task are scheduled in a bucket-by-bucket fashion, if any
        // PlanNode has no more splits, all driver groups must have been seen. However, this
        // won't work if some of the plan node is not large. This could happen in A JOIN B JOIN C,
        // where A and B are large and bucketed, C is smaller and not bucketed.
        if (sourceUpdate.isNoMoreSplits()) {
            System.out.println("HJIN5: WARNING! suspicious line of code HIT");
            schedulingDriverGroups.noMoreDriverGroups();
        }
    }

    private synchronized void scheduleUnpartitionedSource(TaskSource sourceUpdate, Map<PlanNodeId, TaskSource> updatedUnpartitionedSources)
    {
        // create new source
        TaskSource newSource;
        TaskSource currentSource = unpartitionedSources.get(sourceUpdate.getPlanNodeId());
        if (currentSource == null) {
            newSource = sourceUpdate;
        }
        else {
            newSource = currentSource.update(sourceUpdate);
        }

        // only record new source if something changed
        if (newSource != currentSource) {
            unpartitionedSources.put(sourceUpdate.getPlanNodeId(), newSource);
            updatedUnpartitionedSources.put(sourceUpdate.getPlanNodeId(), newSource);
        }
    }

    private synchronized void scheduleUnpartitionedFor(DriverGroupId driverGroupId) {
        for (DriverSplitRunnerFactory driverSplitRunnerFactory : unpartitionedDriverFactories) {
            if (driverSplitRunnerFactory.getExecutionFlowStrategy() != ExecutionFlowStrategy.PER_BUCKET) {
                continue;
            }
            List<DriverSplitRunner> runners = new ArrayList<>();
            for (int i = 0; i < driverSplitRunnerFactory.getDriverInstances().orElse(1); i++) {
                runners.add(driverSplitRunnerFactory.createDriverRunner(null, false, driverGroupId));
            }
            enqueueDrivers(true, runners);
            driverSplitRunnerFactory.noMoreSplitsForDriver(ImmutableList.of(driverGroupId));
        }
    }

    // TODO! rename to enqueue driver split runner
    private synchronized void enqueueDrivers(boolean forceRunSplit, List<DriverSplitRunner> runners)
    {
        // schedule driver to be executed
        List<ListenableFuture<?>> finishedFutures = taskExecutor.enqueueSplits(taskHandle, forceRunSplit, runners);
        checkState(finishedFutures.size() == runners.size(), "Expected %s futures but got %s", runners.size(), finishedFutures.size());

        // when driver completes, update state and fire events
        for (int i = 0; i < finishedFutures.size(); i++) {
            ListenableFuture<?> finishedFuture = finishedFutures.get(i);
            final DriverSplitRunner splitRunner = runners.get(i);

            // record new driver
            status.incrementRemainingDriver(splitRunner.getDriverGroupId());

            Futures.addCallback(finishedFuture, new FutureCallback<Object>()
            {
                @Override
                public void onSuccess(Object result)
                {
                    try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
                        // record driver is finished
                        status.decrementRemainingDriver(splitRunner.getDriverGroupId());

                        checkTaskCompletion();

                        queryMonitor.splitCompletedEvent(taskId, getDriverStats());
                    }
                }

                @Override
                public void onFailure(Throwable cause)
                {
                    try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
                        taskStateMachine.failed(cause);

                        // record driver is finished
                        status.decrementRemainingDriver(splitRunner.getDriverGroupId());

                        // fire failed event with cause
                        queryMonitor.splitFailedEvent(taskId, getDriverStats(), cause);
                    }
                }

                private DriverStats getDriverStats()
                {
                    DriverContext driverContext = splitRunner.getDriverContext();
                    DriverStats driverStats;
                    if (driverContext != null) {
                        driverStats = driverContext.getDriverStats();
                    }
                    else {
                        // split runner did not start successfully
                        driverStats = new DriverStats();
                    }

                    return driverStats;
                }
            }, notificationExecutor);
        }
    }

    public synchronized Set<PlanNodeId> getNoMoreSplits()
    {
        ImmutableSet.Builder<PlanNodeId> noMoreSplits = ImmutableSet.builder();
        for (Entry<PlanNodeId, DriverSplitRunnerFactory> entry : partitionedDriverFactories.entrySet()) {
            if (entry.getValue().isNoMoreSplits()) {
                noMoreSplits.add(entry.getKey());
            }
        }
        for (TaskSource taskSource : unpartitionedSources.values()) {
            if (taskSource.isNoMoreSplits()) {
                noMoreSplits.add(taskSource.getPlanNodeId());
            }
        }
        return noMoreSplits.build();
    }

    public synchronized Set<DriverGroupId> getCompletedDriverGroups()
    {
        return status.getCompleteDriverGroups();
    }

    private synchronized void checkTaskCompletion()
    {
        if (taskStateMachine.getState().isDone()) {
            return;
        }

        // are there more partition splits expected?
        if (!partitionedDriverFactories.values().stream().allMatch(DriverSplitRunnerFactory::isNoMoreSplits)) {
            return;
        }
        // do we still have running tasks?
        if (status.getRemainingDriver() != 0) {
            return;
        }

        // no more output will be created
        outputBuffer.setNoMorePages();

        // are there still pages in the output buffer
        if (!outputBuffer.isFinished()) {
            return;
        }

        // Cool! All done!
        taskStateMachine.finished();
    }

    public void cancel()
    {
        // todo this should finish all input sources and let the task finish naturally
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            taskStateMachine.cancel();
        }
    }

    public void fail(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            taskStateMachine.failed(cause);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskId", taskId)
                .add("remainingDrivers", status.getRemainingDriver())
                .add("unpartitionedSources", unpartitionedSources)
                .toString();
    }

    private class DriverSplitRunnerFactory
    {
        private final DriverFactory driverFactory;
        private final PipelineContext pipelineContext;

        private DriverSplitRunnerFactory(DriverFactory driverFactory)
        {
            this.driverFactory = driverFactory;
            this.pipelineContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver());
        }

        private DriverSplitRunner createDriverRunner(@Nullable ScheduledSplit partitionedSplit, boolean partitioned, DriverGroupId driverGroupId)
        {
            status.incrementPendingCreation(pipelineContext.getPipelineId(), driverGroupId);
            // create driver context immediately so the driver existence is recorded in the stats
            // the number of drivers is used to balance work across nodes
            DriverContext driverContext = pipelineContext.addDriverContext(partitioned, driverGroupId);
            return new DriverSplitRunner(this, driverContext, partitionedSplit, driverGroupId);
        }

        private Driver createDriver(DriverContext driverContext, @Nullable ScheduledSplit partitionedSplit)
        {
            Driver driver = driverFactory.createDriver(driverContext);

            // record driver so other threads add unpartitioned sources can see the driver
            // NOTE: this MUST be done before reading unpartitionedSources, so we see a consistent view of the unpartitioned sources
            drivers.add(new WeakReference<>(driver));

            if (partitionedSplit != null) {
                // TableScanOperator requires partitioned split to be added before the first call to process
                // TODO! this should not use TaskSource
                driver.updateSource(new TaskSource(partitionedSplit.getPlanNodeId(), ImmutableSet.of(partitionedSplit), true));
            }

            // add unpartitioned sources
            for (TaskSource source : unpartitionedSources.values()) {
                driver.updateSource(source);
            }

            status.decrementPendingCreation(pipelineContext.getPipelineId(), driverContext.getDriverGroup());
            closeDriverFactoryIfFullyCreated();

            return driver;
        }

        private void noMoreSplitsForDriver(Iterable<DriverGroupId> driverGroups)
        {
            for (DriverGroupId driverGroupId : driverGroups) {
                System.out.println(String.format("HJIN5 NoMoreSplitsForDG DSRF: Task %s.%s Pipeline %s DriverGroup %s", taskContext.getTaskId().getStageId().getId(), taskContext.getTaskId().getId(), pipelineContext.getPipelineId(), driverGroupId));
                status.setNoMoreSplits(pipelineContext.getPipelineId(), driverGroupId);
            }
        }

        private boolean isNoMoreSplits()
        {
            return status.isNoMoreSplits(pipelineContext.getPipelineId());
        }

        private void setNoMoreSplits()
        {
            System.out.println(String.format("HJIN5: NoMoreSplits DSRF: Task %s.%s Pipeline %s", taskContext.getTaskId().getStageId().getId(), taskContext.getTaskId().getId(), pipelineContext.getPipelineId()));
            status.setNoMoreSplits(pipelineContext.getPipelineId());
            closeDriverFactoryIfFullyCreated();
        }

        private void closeDriverFactoryIfFullyCreated()
        {
            for (DriverGroupId driverGroupId : status.getAndAcknowledgeNoMoreDriversForDriverGroupEvents(pipelineContext.getPipelineId())) {
                System.out.println(String.format("HJIN5: NoMoreDriversForDG DriverFactory: Task %s.%s Pipeline %s DriverGroup %s", taskContext.getTaskId().getStageId().getId(), taskContext.getTaskId().getId(), pipelineContext.getPipelineId(), driverGroupId));
                driverFactory.noMoreDriver(driverGroupId);
            }
            if (status.isNoMoreDrivers(pipelineContext.getPipelineId())) {
                driverFactory.close();
            }
        }

        public ExecutionFlowStrategy getExecutionFlowStrategy()
        {
            return driverFactory.getExecutionFlowStrategy();
        }

        public OptionalInt getDriverInstances()
        {
            return driverFactory.getDriverInstances();
        }
    }

    private static class DriverSplitRunner
            implements SplitRunner
    {
        private final DriverSplitRunnerFactory driverSplitRunnerFactory;
        private final DriverContext driverContext;
        private final DriverGroupId driverGroupId;

        @GuardedBy("this")
        private boolean closed;

        @Nullable
        private final ScheduledSplit partitionedSplit;

        @GuardedBy("this")
        private Driver driver;

        private DriverSplitRunner(DriverSplitRunnerFactory driverSplitRunnerFactory, DriverContext driverContext, @Nullable ScheduledSplit partitionedSplit, DriverGroupId driverGroupId)
        {
            this.driverSplitRunnerFactory = requireNonNull(driverSplitRunnerFactory, "driverFactory is null");
            this.driverContext = requireNonNull(driverContext, "driverContext is null");
            this.partitionedSplit = partitionedSplit;
            this.driverGroupId = requireNonNull(driverGroupId, "driverGroupId is null");
        }

        public synchronized DriverContext getDriverContext()
        {
            if (driver == null) {
                return null;
            }
            return driver.getDriverContext();
        }

        public DriverGroupId getDriverGroupId()
        {
            return driverGroupId;
        }

        @Override
        public synchronized boolean isFinished()
        {
            if (closed) {
                return true;
            }

            if (driver == null) {
                return false;
            }

            return driver.isFinished();
        }

        @Override
        public ListenableFuture<?> processFor(Duration duration)
        {
            Driver driver;
            synchronized (this) {
                // if close() was called before we get here, there's not point in even creating the driver
                if (closed) {
                    return Futures.immediateFuture(null);
                }

                if (this.driver == null) {
                    this.driver = driverSplitRunnerFactory.createDriver(driverContext, partitionedSplit);
                }

                driver = this.driver;
            }

            return driver.processFor(duration);
        }

        @Override
        public String getInfo()
        {
            return (partitionedSplit == null) ? "" : partitionedSplit.getSplit().getInfo().toString();
        }

        @Override
        public void close()
        {
            Driver driver;
            synchronized (this) {
                closed = true;
                driver = this.driver;
            }

            if (driver != null) {
                driver.close();
            }
        }
    }

    private static final class RemoveTaskHandleWhenDone
            implements StateChangeListener<TaskState>
    {
        private final TaskExecutor taskExecutor;
        private final TaskHandle taskHandle;

        private RemoveTaskHandleWhenDone(TaskExecutor taskExecutor, TaskHandle taskHandle)
        {
            this.taskExecutor = requireNonNull(taskExecutor, "taskExecutor is null");
            this.taskHandle = requireNonNull(taskHandle, "taskHandle is null");
        }

        @Override
        public void stateChanged(TaskState newState)
        {
            if (newState.isDone()) {
                taskExecutor.removeTask(taskHandle);
            }
        }
    }

    private static final class CheckTaskCompletionOnBufferFinish
            implements StateChangeListener<BufferState>
    {
        private final WeakReference<SqlTaskExecution> sqlTaskExecutionReference;

        public CheckTaskCompletionOnBufferFinish(SqlTaskExecution sqlTaskExecution)
        {
            // we are only checking for completion of the task, so don't hold up GC if the task is dead
            this.sqlTaskExecutionReference = new WeakReference<>(sqlTaskExecution);
        }

        @Override
        public void stateChanged(BufferState newState)
        {
            if (newState == BufferState.FINISHED) {
                SqlTaskExecution sqlTaskExecution = sqlTaskExecutionReference.get();
                if (sqlTaskExecution != null) {
                    sqlTaskExecution.checkTaskCompletion();
                }
            }
        }
    }

    private void checkHoldsLock()
    {
        // This method serves a similar purpose at runtime as GuardedBy on method serves during static analysis.
        // This method should not have significant performance impact. If it does, it may be reasonably to remove this method.
        // This intentionally does not use checkState.
        if (!Thread.holdsLock(this)) {
            throw new IllegalStateException(format("Thread must hold a lock on the %s", getClass().getSimpleName()));
        }
    }

    private /*static*/ class Status
    {
        // remainingDrivers: Number of driver split runners
        // pendingCreation:
        // noMoreSplits:

        private final int pipelineCount;

        // For these 3 perX fields, they are populated lazily. If enumeration operations on the
        // map can lead to side effects, no new entries can be created after such enumeration has
        // happened. Otherwise, the order of entry creation and the enumeration operation will
        // lead to different outcome.
        private final Map<Integer, Map<DriverGroupId, PerPlanNodeAndDriverGroupStatus>> perPlanNodeAndDriverGroup;
        private final Map<Integer, PerPlanNodeStatus> perPlanNode;
        private final Map<DriverGroupId, PerDriverGroupStatus> perDriverGroup = new HashMap<>();

        private int overallRemainingDriver;
        private int overallNoMoreSplitsDriverGroups;
        private int driverGroupCount;
        private boolean noMoreDriverGroups;

        public Status(List<Integer> pipelineIdList)
        {
            pipelineCount = pipelineIdList.size();
            ImmutableMap.Builder<Integer, Map<DriverGroupId, PerPlanNodeAndDriverGroupStatus>> perPlanNodeAndDriverGroup = ImmutableMap.builder();
            ImmutableMap.Builder<Integer, PerPlanNodeStatus> perPlanNode = ImmutableMap.builder();
            for (Integer pipelineId : pipelineIdList) {
                perPlanNodeAndDriverGroup.put(pipelineId, new HashMap<>());
                perPlanNode.put(pipelineId, new PerPlanNodeStatus());
            }
            this.perPlanNodeAndDriverGroup = perPlanNodeAndDriverGroup.build();
            this.perPlanNode = perPlanNode.build();
        }

        public synchronized void incrementPendingCreation(int pipelineId, DriverGroupId driverGroupId)
        {
            checkState(!per(pipelineId, driverGroupId).noMoreSplits, "Cannot increment pendingCreation for Pipeline %s DriverGroup %s. NoMoreSplits is set.", pipelineId, driverGroupId);
            per(pipelineId, driverGroupId).pendingCreation++;
            per(pipelineId).pendingCreation++;
        }

        public synchronized void decrementPendingCreation(int pipelineId, DriverGroupId driverGroupId)
        {
            checkState(per(pipelineId, driverGroupId).pendingCreation > 0, "Cannot decrement pendingCreation for Pipeline %s DriverGroup %s. Value is 0.", pipelineId, driverGroupId);
            per(pipelineId, driverGroupId).pendingCreation--;
            if (per(pipelineId, driverGroupId).pendingCreation == 0 && per(pipelineId, driverGroupId).noMoreSplits) {
                System.out.println(String.format("HJIN9: NoMoreDriversForDG Event: Task %s.%s Pipeline %s DriverGroup %s", taskContext.getTaskId().getStageId().getId(), taskContext.getTaskId().getId(), pipelineId, driverGroupId));
                per(pipelineId).unacknowledgedNoMoreDriversForDriverGroupEvents.add(driverGroupId);
            }
            per(pipelineId).pendingCreation--;
        }

        public synchronized int getPendingCreation(int pipelineId, DriverGroupId driverGroupId)
        {
            return per(pipelineId, driverGroupId).pendingCreation;
        }

        public synchronized int getPendingCreation(int pipelineId)
        {
            return per(pipelineId).pendingCreation;
        }

        public synchronized void incrementRemainingDriver(DriverGroupId driverGroupId)
        {
            checkState(!isNoMoreSplits(driverGroupId), "Cannot increment remainingDriver for DriverGroup %s. NoMoreSplits is set.", driverGroupId);
            per(driverGroupId).remainingDriver++;
            overallRemainingDriver++;
        }

        public synchronized void decrementRemainingDriver(DriverGroupId driverGroupId)
        {
            checkState(per(driverGroupId).remainingDriver > 0, "Cannot decrement remainingDriver for DriverGroup %s. Value is 0.", driverGroupId);
            per(driverGroupId).remainingDriver--;
            overallRemainingDriver--;
        }

        public synchronized int getRemainingDriver(DriverGroupId driverGroupId)
        {
            return per(driverGroupId).remainingDriver;
        }

        public synchronized int getRemainingDriver()
        {
            return overallRemainingDriver;
        }

        public synchronized void setNoMoreSplits(int pipelineId, DriverGroupId driverGroupId)
        {
            if (per(pipelineId, driverGroupId).noMoreSplits) {
                return;
            }
            per(pipelineId, driverGroupId).noMoreSplits = true;
            if (per(pipelineId, driverGroupId).pendingCreation == 0) {
                System.out.println(String.format("HJIN9: NoMoreDriversForDG Event: Pipeline %s DriverGroup %s", pipelineId, driverGroupId));
                per(pipelineId).unacknowledgedNoMoreDriversForDriverGroupEvents.add(driverGroupId);
            }
            per(pipelineId).noMoreSplitsDriverGroups++;
            per(driverGroupId).noMoreSplitsPipelines++;
            if (per(driverGroupId).noMoreSplitsPipelines == pipelineCount) {
                overallNoMoreSplitsDriverGroups++;
            }
        }

        public synchronized void setNoMoreSplits(int pipelineId)
        {
            // If any of the source nodes is in no more splits state, there cannot be any more driver groups?
            noMoreDriverGroups = true;

            // TODO!
            // For BbB drivers, this should only happen after setNoMoreSplits has been invoked on every driverGroupId
            // For all-at-once drivers, this should delegate to setNoMoreSplits(pipelineId, empty())
            // The implementation below is temporary
            checkState(perDriverGroup.size() == driverGroupCount);
            //checkState(perPlanNodeAndDriverGroup.get(pipelineId).size() == driverGroupCount, "driverGroupCount: %s, count for pipeline %s: %s", driverGroupCount, pipelineId, perPlanNodeAndDriverGroup.get(pipelineId).size());
            for (DriverGroupId driverGroupId : perDriverGroup.keySet()) {
                setNoMoreSplits(pipelineId, driverGroupId);
            }
        }

        public synchronized boolean isNoMoreSplits(int pipelineId, DriverGroupId driverGroupId)
        {
            return per(pipelineId, driverGroupId).noMoreSplits;
        }

        public synchronized boolean isNoMoreSplits(int pipelineId)
        {
            return noMoreDriverGroups && per(pipelineId).noMoreSplitsDriverGroups == driverGroupCount;
        }

        public synchronized boolean isNoMoreSplits(DriverGroupId driverGroupId)
        {
            return per(driverGroupId).noMoreSplitsPipelines == pipelineCount;
        }

        public synchronized boolean isNoMoreSplits()
        {
            return noMoreDriverGroups && overallNoMoreSplitsDriverGroups == driverGroupCount;
        }

        public synchronized List<DriverGroupId> getAndAcknowledgeNoMoreDriversForDriverGroupEvents(int pipelineId)
        {
            List<DriverGroupId> result = ImmutableList.copyOf(per(pipelineId).unacknowledgedNoMoreDriversForDriverGroupEvents);
            per(pipelineId).unacknowledgedNoMoreDriversForDriverGroupEvents.clear();
            return result;
        }

        public synchronized boolean isNoMoreDrivers(int pipelineId)
        {
            return isNoMoreSplits(pipelineId) && getPendingCreation(pipelineId) == 0;
        }

        public synchronized Set<DriverGroupId> getCompleteDriverGroups()
        {
            ImmutableSet.Builder<DriverGroupId> result = ImmutableSet.builder();
            for (DriverGroupId driverGroupId : perDriverGroup.keySet()) {
                boolean noMoreSplits = isNoMoreSplits(driverGroupId);
                int remainingDriver = getRemainingDriver(driverGroupId);
                if (noMoreSplits && remainingDriver == 4) {
                    result = result;
                }
                if (noMoreSplits && remainingDriver == 0) {
                    result.add(driverGroupId);
                }
            }
            return result.build();
        }

        private PerPlanNodeAndDriverGroupStatus per(int pipelineId, DriverGroupId driverGroupId)
        {
            return perPlanNodeAndDriverGroup.get(pipelineId).computeIfAbsent(driverGroupId, ignored -> new PerPlanNodeAndDriverGroupStatus());
        }

        private PerPlanNodeStatus per(int pipelineId)
        {
            return perPlanNode.get(pipelineId);
        }

        private PerDriverGroupStatus per(DriverGroupId driverGroupId)
        {
            if (perDriverGroup.containsKey(driverGroupId)) {
                return perDriverGroup.get(driverGroupId);
            }
            checkState(!noMoreDriverGroups, "noMoreDriverGroups is set");
            driverGroupCount++;
            PerDriverGroupStatus result = new PerDriverGroupStatus();
            perDriverGroup.put(driverGroupId, result);
            return result;
        }
    }

    private static class PerPlanNodeAndDriverGroupStatus
    {
        int pendingCreation;
        boolean noMoreSplits;
    }

    private static class PerPlanNodeStatus
    {
        int pendingCreation;
        int noMoreSplitsDriverGroups;
        List<DriverGroupId> unacknowledgedNoMoreDriversForDriverGroupEvents = new ArrayList<>();
    }

    private static class PerDriverGroupStatus
    {
        int remainingDriver;
        int noMoreSplitsPipelines;
    }
}
