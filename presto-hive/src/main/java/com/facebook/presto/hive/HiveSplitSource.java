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
package com.facebook.presto.hive;

import com.facebook.presto.hive.util.AsyncQueue;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;

import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILE_NOT_FOUND;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static com.facebook.presto.hive.HiveSplitSource.StateKind.CLOSED;
import static com.facebook.presto.hive.HiveSplitSource.StateKind.FAILED;
import static com.facebook.presto.hive.HiveSplitSource.StateKind.INITIAL;
import static com.facebook.presto.hive.HiveSplitSource.StateKind.NO_MORE_SPLITS;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.failedFuture;
import static java.util.Objects.requireNonNull;

class HiveSplitSource
        implements ConnectorSplitSource
{
    private final PerDriverGroup<AsyncQueue<ConnectorSplit>> queues;
    private final HiveSplitLoader splitLoader;
    private final AtomicReference<State> stateReference;

    private HiveSplitSource(PerDriverGroup<AsyncQueue<ConnectorSplit>> queues, HiveSplitLoader splitLoader, AtomicReference<State> stateReference)
    {
        this.queues = requireNonNull(queues, "queues is null");
        this.splitLoader = requireNonNull(splitLoader, "splitLoader is null");
        this.stateReference = requireNonNull(stateReference, "stateReference is null");
    }

    public static HiveSplitSource allAtOnce(int maxOutstandingSplits, HiveSplitLoader splitLoader, Executor executor)
    {
        AtomicReference<State> stateReference = new AtomicReference<>(State.initial());
        return new HiveSplitSource(
                new PerDriverGroup<AsyncQueue<ConnectorSplit>>()
                {
                    private final AsyncQueue<ConnectorSplit> queue = new AsyncQueue<>(maxOutstandingSplits, executor);

                    @Override
                    public CompletableFuture<?> offer(OptionalInt driverGroupId, ConnectorSplit connectorSplit)
                    {
                        // driverGroupId can be non-empty because BackgroundHiveSplitLoader does not have knowledge of execution plan
                        return queue.offer(connectorSplit);
                    }

                    @Override
                    public CompletableFuture<List<ConnectorSplit>> getBatchAsync(OptionalInt driverGroupId, int maxSize)
                    {
                        // driverGroupId can be non-empty because BackgroundHiveSplitLoader does not have knowledge of execution plan
                        return queue.getBatchAsync(maxSize);
                    }

                    @Override
                    public void finish()
                    {
                        queue.finish();
                    }

                    @Override
                    public int getOutstandingSplitCount()
                    {
                        return queue.size();
                    }
                },
                splitLoader,
                stateReference);
    }

    public static HiveSplitSource bucketed(int maxOutstandingSplitsPerBucket, HiveSplitLoader splitLoader, Executor executor)
    {
        AtomicReference<State> stateReference = new AtomicReference<>(State.initial());
        return new HiveSplitSource(
                new PerDriverGroup<AsyncQueue<ConnectorSplit>>()
                {
                    private final Map<OptionalInt, AsyncQueue<ConnectorSplit>> queues = new ConcurrentHashMap<>();
                    private final AtomicInteger outstandingSplitCount = new AtomicInteger();

                    @Override
                    public CompletableFuture<?> offer(OptionalInt driverGroupId, ConnectorSplit connectorSplit)
                    {
                        AsyncQueue<ConnectorSplit> queue = queueFor(driverGroupId);
                        outstandingSplitCount.incrementAndGet();
                        return queue.offer(connectorSplit);
                    }

                    @Override
                    public CompletableFuture<List<ConnectorSplit>> getBatchAsync(OptionalInt driverGroupId, int maxSize)
                    {
                        AsyncQueue<ConnectorSplit> queue = queueFor(driverGroupId);
                        return queue.getBatchAsync(maxSize).thenApply(connectorSplits -> {
                            outstandingSplitCount.addAndGet(-connectorSplits.size());
                            return connectorSplits;
                        });
                    }

                    @Override
                    public void finish()
                    {
                        queues.values().forEach(AsyncQueue::finish);
                    }

                    @Override
                    public int getOutstandingSplitCount()
                    {
                        return outstandingSplitCount.get();
                    }

                    public AsyncQueue<ConnectorSplit> queueFor(OptionalInt driverGroupId)
                    {
                        return queues.computeIfAbsent(driverGroupId, ignored -> {
                            State state = stateReference.get();
                            if (state.getKind() != INITIAL) {
                                throw new IllegalStateException();
                            }
                            return new AsyncQueue<>(maxOutstandingSplitsPerBucket, executor);
                        });
                    }
                },
                splitLoader,
                stateReference);
    }

    @VisibleForTesting
    int getOutstandingSplitCount()
    {
        return queues.getOutstandingSplitCount();
    }

    CompletableFuture<?> addToQueue(Iterator<? extends ConnectorSplit> splits)
    {
        CompletableFuture<?> lastResult = CompletableFuture.completedFuture(null);
        while (splits.hasNext()) {
            ConnectorSplit split = splits.next();
            lastResult = addToQueue(split);
        }
        return lastResult;
    }

    CompletableFuture<?> addToQueue(ConnectorSplit split)
    {
        if (stateReference.get().getKind() != INITIAL) {
            return CompletableFuture.completedFuture(null);
        }
        OptionalInt driverGroupId = split.getDriverGroupId();
        return queues.offer(driverGroupId, split);
    }

    // TODO! rename to noMoreSplits
    void finished()
    {
        if (setIf(stateReference, State.noMoreSplits(), state -> state.getKind() == INITIAL)) {
            // add finish the queue
            queues.finish();

            // no need to process any more jobs
            splitLoader.stop();
        }
    }

    void fail(Throwable e)
    {
        // only record the first error message
        if (setIf(stateReference, State.failed(e), state -> state.getKind() == INITIAL)) {
            // add finish the queue
            queues.finish();

            // no need to process any more jobs
            splitLoader.stop();
        }
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
    {
        return getNextBatch(OptionalInt.empty(), maxSize).thenApply(ConnectorSplitBatch::getSplits);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(OptionalInt driverGroupId, int maxSize)
    {
        State state = stateReference.get();
        checkState(state.getKind() != CLOSED, "HiveSplitSource is already closed");
        if (state.getKind() == FAILED) {
            System.out.println(String.format("HJIN1: fail"));
            return failedFuture(state.getThrowable());
        }

        CompletableFuture<List<ConnectorSplit>> batch = queues.getBatchAsync(driverGroupId, maxSize);
        return batch.thenApply(splits -> {
            //System.out.println(String.format("HJIN1: fetch %s", splits.size()));
            // Use the `state` got at the beginning of the function (outside this lambda). Otherwise, it's not thread-safe.
            return new ConnectorSplitBatch(splits, splits.isEmpty() && state.getKind() == NO_MORE_SPLITS);
        });
    }

    @Override
    public boolean isFinished()
    {
        State state = stateReference.get();

        if (state.getKind() == FAILED) {
            throw propagatePrestoException(state.getThrowable());
        }
        if (state.getKind() == INITIAL) {
            return false;
        }
        return queues.getOutstandingSplitCount() == 0;
    }

    @Override
    public void close()
    {
        if (setIf(stateReference, State.closed(), state -> state.getKind() == INITIAL || state.getKind() == NO_MORE_SPLITS)) {
            // add finish the queue
            queues.finish();

            // no need to process any more jobs
            splitLoader.stop();
        }
    }

    private static <T> boolean setIf(AtomicReference<T> atomicReference, T newValue, Predicate<T> predicate)
    {
        while (true) {
            T current = atomicReference.get();
            if (!predicate.test(current)) {
                return false;
            }
            if (atomicReference.compareAndSet(current, newValue)) {
                return true;
            }
        }
    }

    private static RuntimeException propagatePrestoException(Throwable throwable)
    {
        if (throwable instanceof PrestoException) {
            throw (PrestoException) throwable;
        }
        if (throwable instanceof FileNotFoundException) {
            throw new PrestoException(HIVE_FILE_NOT_FOUND, throwable);
        }
        throw new PrestoException(HIVE_UNKNOWN_ERROR, throwable);
    }

    interface PerDriverGroup<T>
    {
        CompletableFuture<?> offer(OptionalInt driverGroupId, ConnectorSplit connectorSplit);

        CompletableFuture<List<ConnectorSplit>> getBatchAsync(OptionalInt driverGroupId, int maxSize);

        void finish();

        int getOutstandingSplitCount();
    }

    static class State
    {
        private final StateKind kind;
        private final Throwable throwable;

        private State(StateKind kind, Throwable throwable)
        {
            this.kind = kind;
            this.throwable = throwable;
        }

        public StateKind getKind()
        {
            return kind;
        }

        public Throwable getThrowable()
        {
            checkState(throwable != null);
            return throwable;
        }

        public static State initial()
        {
            return new State(INITIAL, null);
        }

        public static State noMoreSplits()
        {
            return new State(NO_MORE_SPLITS, null);
        }

        public static State failed(Throwable throwable)
        {
            return new State(FAILED, throwable);
        }

        public static State closed()
        {
            return new State(CLOSED, null);
        }
    }

    enum StateKind {
        INITIAL,
        NO_MORE_SPLITS,
        FAILED,
        CLOSED,
    }
}
