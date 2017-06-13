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
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Node;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.NodePartitionMap;
import com.facebook.presto.sql.planner.plan.ExecutionFlowStrategy;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static java.util.Objects.requireNonNull;

public class FixedSourcePartitionedScheduler
        implements StageScheduler
{
    private static final Logger log = Logger.get(FixedSourcePartitionedScheduler.class);

    private final SqlStageExecution stage;
    private final NodePartitionMap partitioning;
    private final List<SourcePartitionedScheduler> sourcePartitionedSchedulers;
    private boolean scheduledTasks;

    public FixedSourcePartitionedScheduler(
            SqlStageExecution stage,
            Map<PlanNodeId, SplitSource> splitSources,
            Map<PlanNodeId, ExecutionFlowStrategy> executionFlowStrategies,
            List<PlanNodeId> schedulingOrder,
            NodePartitionMap partitioning,
            int splitBatchSize,
            NodeSelector nodeSelector)
    {
        requireNonNull(stage, "stage is null");
        requireNonNull(splitSources, "splitSources is null");
        requireNonNull(partitioning, "partitioning is null");

        this.stage = stage;
        this.partitioning = partitioning;

        checkArgument(splitSources.keySet().equals(ImmutableSet.copyOf(schedulingOrder)));
        checkArgument(executionFlowStrategies.keySet().equals(ImmutableSet.copyOf(schedulingOrder)));

        FixedSplitPlacementPolicy splitPlacementPolicy = new FixedSplitPlacementPolicy(nodeSelector, partitioning, stage::getAllTasks);
        sourcePartitionedSchedulers = schedulingOrder.stream()
                .map(planNodeId -> new SourcePartitionedScheduler(stage, planNodeId, splitSources.get(planNodeId), splitPlacementPolicy, splitBatchSize, executionFlowStrategies.get(planNodeId)))
                .collect(Collectors.toList());
    }

    @Override
    public ScheduleResult schedule()
    {
        // schedule a task on every node in the distribution
        List<RemoteTask> newTasks = ImmutableList.of();
        if (!scheduledTasks) {
            newTasks = partitioning.getPartitionToNode().entrySet().stream()
                    .map(entry -> stage.scheduleTask(entry.getValue(), entry.getKey()))
                    .collect(toImmutableList());
            scheduledTasks = true;
        }

        boolean allBlocked = true;
        List<ListenableFuture<?>> blocked = new ArrayList<>();
        Set<ScheduleResult.BlockedReason> blockedReasons = new HashSet<>();
        int splitsScheduled = 0;

        Iterator<SourcePartitionedScheduler> schedulerIterator = sourcePartitionedSchedulers.iterator();
        List<OptionalInt> driverGroupsToStart = ImmutableList.of();
        while (schedulerIterator.hasNext()) {
            SourcePartitionedScheduler sourcePartitionedScheduler = schedulerIterator.next();

            sourcePartitionedScheduler.startDriverGroups(driverGroupsToStart);

            ScheduleResult schedule = sourcePartitionedScheduler.schedule();
            splitsScheduled += schedule.getSplitsScheduled();
            if (schedule.getBlockedReason().isPresent()) {
                blocked.add(schedule.getBlocked());
                blockedReasons.add(schedule.getBlockedReason().get());
            }
            else {
                verify(schedule.getBlocked().isDone(), "blockedReason not provided when scheduler is blocked");
                allBlocked = false;
            }

            driverGroupsToStart = sourcePartitionedScheduler.getAndCleanUpCompletedDriverGroups();

            if (schedule.isFinished()) {
                schedulerIterator.remove();
                sourcePartitionedScheduler.close();
            }
        }

        if (allBlocked) {
            // TODO: put the entire list in the reasons section
            ScheduleResult.BlockedReason blockedReason = blockedReasons.iterator().next();
            return new ScheduleResult(sourcePartitionedSchedulers.isEmpty(), newTasks, whenAnyComplete(blocked), blockedReason, splitsScheduled);
        }
        else {
            return new ScheduleResult(sourcePartitionedSchedulers.isEmpty(), newTasks, splitsScheduled);
        }
    }

    @Override
    public void close()
    {
        for (SourcePartitionedScheduler sourcePartitionedScheduler : sourcePartitionedSchedulers) {
            try {
                sourcePartitionedScheduler.close();
            }
            catch (Throwable t) {
                log.warn(t, "Error closing split source");
            }
        }
        sourcePartitionedSchedulers.clear();
    }

    public static class FixedSplitPlacementPolicy
            implements SplitPlacementPolicy
    {
        private final NodeSelector nodeSelector;
        private final NodePartitionMap partitioning;
        private final Supplier<? extends List<RemoteTask>> remoteTasks;

        public FixedSplitPlacementPolicy(NodeSelector nodeSelector,
                NodePartitionMap partitioning,
                Supplier<? extends List<RemoteTask>> remoteTasks)
        {
            this.nodeSelector = nodeSelector;
            this.partitioning = partitioning;
            this.remoteTasks = remoteTasks;
        }

        @Override
        public SplitPlacementResult computeAssignments(Set<Split> splits)
        {
            return nodeSelector.computeAssignments(splits, remoteTasks.get(), partitioning);
        }

        @Override
        public void lockDownNodes()
        {
        }

        @Override
        public List<Node> allNodes()
        {
            return ImmutableList.copyOf(partitioning.getPartitionToNode().values());
        }

        public Node getNodeForBucket(int bucketId)
        {
            return partitioning.getPartitionToNode().get(partitioning.getBucketToPartition()[bucketId]);
        }
    }
}
