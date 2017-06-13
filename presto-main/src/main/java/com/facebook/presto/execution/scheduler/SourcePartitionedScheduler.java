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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.scheduler.FixedSourcePartitionedScheduler.FixedSplitPlacementPolicy;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Node;
import com.facebook.presto.split.EmptySplit;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.split.SplitSource.SplitBatch;
import com.facebook.presto.sql.planner.plan.ExecutionFlowStrategy;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalInt;
import java.util.Set;

import static com.facebook.presto.execution.scheduler.ScheduleResult.BlockedReason.SPLIT_QUEUES_FULL;
import static com.facebook.presto.execution.scheduler.ScheduleResult.BlockedReason.WAITING_FOR_SOURCE;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static java.util.Objects.requireNonNull;

public class SourcePartitionedScheduler
        implements StageScheduler
{
    private enum State
    {
        INITIALIZED,
        SPLITS_SCHEDULED,
        FINISHED
    }

    private final SqlStageExecution stage;
    private final SplitSource splitSource;
    private final SplitPlacementPolicy splitPlacementPolicy;
    private final int splitBatchSize;
    private final PlanNodeId partitionedNode;

    private final Map<OptionalInt, ScheduleGroup> scheduleGroups = new HashMap<>();
    private State state = State.INITIALIZED;

    public SourcePartitionedScheduler(
            SqlStageExecution stage,
            PlanNodeId partitionedNode,
            SplitSource splitSource,
            SplitPlacementPolicy splitPlacementPolicy,
            int splitBatchSize,
            ExecutionFlowStrategy executionFlowStrategy)
    {
        this.stage = requireNonNull(stage, "stage is null");
        this.splitSource = requireNonNull(splitSource, "splitSource is null");
        this.splitPlacementPolicy = requireNonNull(splitPlacementPolicy, "splitPlacementPolicy is null");

        checkArgument(splitBatchSize > 0, "splitBatchSize must be at least one");
        this.splitBatchSize = splitBatchSize;

        this.partitionedNode = partitionedNode;

        if (executionFlowStrategy == ExecutionFlowStrategy.PER_BUCKET) {
            startDriverGroups(ImmutableList.of(OptionalInt.of(0), OptionalInt.of(1)));
        }
        else {
            startDriverGroups(ImmutableList.of(OptionalInt.empty()));
        }
    }

    @Override
    public synchronized ScheduleResult schedule()
    {
        // TODO! return BLOCKED when there are no driver groups active to avoid busy waiting

        int overallSplitAssignmentCount = 0;
        ImmutableSet.Builder<RemoteTask> overallNewTasks = ImmutableSet.builder();
        List<ListenableFuture<?>> overallBlockedPlacements = new ArrayList<>();

        for (Entry<OptionalInt, ScheduleGroup> entry : scheduleGroups.entrySet()) {
            OptionalInt driverGroupId = entry.getKey();
            ScheduleGroup scheduleGroup = entry.getValue();
            Set<Split> pendingSplits = scheduleGroup.pendingSplits;

            boolean belowLowWatermark = pendingSplits.size() <= splitBatchSize / 2;
            if (belowLowWatermark) {
                // try to get the next batch
                if (scheduleGroup.batchFuture == null) {
                    if (splitSource.isFinished()) {
                        checkArgument(!driverGroupId.isPresent(), "Milestone when hit");
                        return handleNoMoreSplits(driverGroupId);
                    }
                    scheduleGroup.batchFuture = splitSource.getNextBatch(driverGroupId, splitBatchSize - pendingSplits.size());

                    long start = System.nanoTime();
                    Futures.addCallback(scheduleGroup.batchFuture, new FutureCallback<SplitBatch>()
                    {
                        @Override
                        public void onSuccess(SplitBatch result)
                        {
                            stage.recordGetSplitTime(start);
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                        }
                    });
                }

                if (scheduleGroup.batchFuture.isDone()) {
                    SplitBatch nextSplits = getFutureValue(scheduleGroup.batchFuture);
                    scheduleGroup.batchFuture = null;

                    pendingSplits.addAll(nextSplits.getSplits());
                    if (nextSplits.isNoMoreSplits() && scheduleGroup.state == ScheduleGroupState.INITIAL) {
                        scheduleGroup.state = ScheduleGroupState.NO_MORE_SPLITS;
                        if (driverGroupId.isPresent()) {
                            scheduleGroup.node = ((FixedSplitPlacementPolicy) splitPlacementPolicy).getNodeForBucket(driverGroupId.getAsInt());
                        }
                    }
                }
                else {
                    if (pendingSplits.isEmpty()) {
                        // wrap batch future so cancellation is not propagated
                        ListenableFuture<SplitBatch> blocked = nonCancellationPropagating(scheduleGroup.batchFuture);
                        return new ScheduleResult(false, ImmutableSet.of(), blocked, WAITING_FOR_SOURCE, 0);
                    }
                }
            }

            if (!pendingSplits.isEmpty() && state == State.INITIALIZED) {
                state = State.SPLITS_SCHEDULED;
            }

            // calculate placements for splits
            SplitPlacementResult splitPlacementResult = splitPlacementPolicy.computeAssignments(pendingSplits);
            Multimap<Node, Split> splitAssignment = splitPlacementResult.getAssignments();

            // remove splits with successful placements
            pendingSplits.removeAll(splitAssignment.values());
            overallSplitAssignmentCount += splitAssignment.size();
            if (!pendingSplits.isEmpty()) {
                overallBlockedPlacements.add(splitPlacementResult.getBlocked());
            }

            // TODO! add comment
            Map<Node, OptionalInt> noMoreSplitsNotification = ImmutableMap.of();
            if (pendingSplits.isEmpty() && scheduleGroup.state == ScheduleGroupState.NO_MORE_SPLITS) {
                noMoreSplitsNotification = ImmutableMap.of(scheduleGroup.node, driverGroupId);
                scheduleGroup.state = ScheduleGroupState.DONE;
            }

            // assign the splits with successful placements
            // TODO! notify the task that a particular driver is done
            // TODO! assert that (in assignSplits) only 1 node is assigned (assuming driver group scheduling)
            Set<RemoteTask> newTasks = assignSplits(splitAssignment, noMoreSplitsNotification);
            overallNewTasks.addAll(newTasks);
        }

        if (overallBlockedPlacements.isEmpty()) {
            boolean finished = false;
            // all splits assigned - check if the source is finished
            finished = splitSource.isFinished();
            if (finished) {
                splitSource.close();
            }
            return new ScheduleResult(
                    finished,
                    overallNewTasks.build(),
                    overallSplitAssignmentCount);
        }

        overallNewTasks.addAll(finalizeTaskCreationIfNecessary());

        return new ScheduleResult(
                false,
                overallNewTasks.build(),
                getFirstCompleteAndCancelOthers(overallBlockedPlacements),
                SPLIT_QUEUES_FULL,
                overallSplitAssignmentCount);
    }

    private ScheduleResult handleNoMoreSplits(OptionalInt driverGroupId)
    {
        checkArgument(!driverGroupId.isPresent()); // caller has the same check
        switch (state) {
            case INITIALIZED:
                // we have not scheduled a single split so far
                return scheduleEmptySplit();
            case SPLITS_SCHEDULED:
                state = State.FINISHED;
                splitSource.close();
                return new ScheduleResult(true, ImmutableSet.of(), 0);
            default:
                throw new IllegalStateException("SourcePartitionedScheduler expected to be in INITIALIZED or SPLITS_SCHEDULED state but is in " + state);
        }
    }

    @Override
    public void close()
    {
        splitSource.close();
    }

    public void startDriverGroups(List<OptionalInt> driverGroupIds)
    {
        for (OptionalInt driverGroupId : driverGroupIds) {
            scheduleGroups.put(driverGroupId, new ScheduleGroup(driverGroupId));
        }
    }

    public List<OptionalInt> getAndCleanUpCompletedDriverGroups()
    {
        //TODO! include all driver groups here if the the scheduler is done.
        ImmutableList.Builder<OptionalInt> result = ImmutableList.builder();
        Iterator<Entry<OptionalInt, ScheduleGroup>> entryIterator = scheduleGroups.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Entry<OptionalInt, ScheduleGroup> entry = entryIterator.next();
            if (entry.getValue().state == ScheduleGroupState.DONE) {
                result.add(entry.getKey());
                entryIterator.remove();
            }
        }
        return result.build();
    }

    private ScheduleResult scheduleEmptySplit()
    {
        state = State.SPLITS_SCHEDULED;

        List<Node> nodes = splitPlacementPolicy.allNodes();
        checkCondition(!nodes.isEmpty(), NO_NODES_AVAILABLE, "No nodes available to run query");
        Node node = nodes.iterator().next();

        Split emptySplit = new Split(
                splitSource.getConnectorId(),
                splitSource.getTransactionHandle(),
                new EmptySplit(splitSource.getConnectorId()));
        Set<RemoteTask> emptyTask = assignSplits(ImmutableMultimap.of(node, emptySplit), ImmutableMap.of());
        return new ScheduleResult(false, emptyTask, 1);
    }

    private Set<RemoteTask> assignSplits(Multimap<Node, Split> splitAssignment, Map<Node, OptionalInt> noMoreSplitsNotification)
    {
        ImmutableSet.Builder<RemoteTask> newTasks = ImmutableSet.builder();

        ImmutableSet<Node> nodes = ImmutableSet.<Node>builder()
                .addAll(splitAssignment.keySet())
                .addAll(noMoreSplitsNotification.keySet())
                .build();
        for (Node node : nodes) {
            // source partitioned tasks can only receive broadcast data; otherwise it would have a different distribution
            ImmutableMultimap<PlanNodeId, Split> splits = ImmutableMultimap.<PlanNodeId, Split>builder()
                    .putAll(partitionedNode, splitAssignment.get(node))
                    .build();
            ImmutableMap.Builder<PlanNodeId, OptionalInt> noMoreSplits = ImmutableMap.builder();
            if (noMoreSplitsNotification.containsKey(node)) {
                noMoreSplits.put(partitionedNode, noMoreSplitsNotification.get(node));
            }
            newTasks.addAll(stage.scheduleSplits(
                    node,
                    splits,
                    noMoreSplits.build()));
        }
        return newTasks.build();
    }

    private Set<RemoteTask> finalizeTaskCreationIfNecessary()
    {
        // only lock down tasks if there is a sub stage that could block waiting for this stage to create all tasks
        if (stage.getFragment().isLeaf()) {
            return ImmutableSet.of();
        }

        splitPlacementPolicy.lockDownNodes();

        Set<Node> scheduledNodes = stage.getScheduledNodes();
        Set<RemoteTask> newTasks = splitPlacementPolicy.allNodes().stream()
                .filter(node -> !scheduledNodes.contains(node))
                .flatMap(node -> stage.scheduleSplits(node, ImmutableMultimap.of(), ImmutableMap.of()).stream())
                .collect(toImmutableSet());

        // notify listeners that we have scheduled all tasks so they can set no more buffers or exchange splits
        stage.transitionToSchedulingSplits();

        return newTasks;
    }

    private static ListenableFuture<?> getFirstCompleteAndCancelOthers(List<ListenableFuture<?>> blockedFutures)
    {
        // wait for the first task to unblock and then cancel all futures to free up resources
        ListenableFuture<?> result = whenAnyComplete(blockedFutures);
        /*
        result.addListener(
                () -> {
                    for (ListenableFuture<?> blockedFuture : blockedFutures) {
                        blockedFuture.cancel(true);
                    }
                },
                directExecutor());
        */
        return result;
    }

    private static class ScheduleGroup
    {
        // TODO! this field is unused
        public final OptionalInt driverGroupId;
        public Node node = null;
        public ListenableFuture<SplitBatch> batchFuture = null;
        public Set<Split> pendingSplits = new HashSet<>();
        public ScheduleGroupState state = ScheduleGroupState.INITIAL;

        public ScheduleGroup(OptionalInt driverGroupId)
        {
            this.driverGroupId = driverGroupId;
        }
    }

    private enum ScheduleGroupState {
        INITIAL,
        NO_MORE_SPLITS, // no more splits will be added to pendingSplits set
        DONE // notified worker that it has received all splits
    }
}
