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
package com.facebook.presto.operator.exchange;

import com.facebook.presto.execution.DriverGroupId;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.facebook.presto.operator.exchange.LocalExchangeSink.finishedLocalExchangeSink;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class LocalExchange
{
    private static final DataSize DEFAULT_MAX_BUFFERED_BYTES = new DataSize(32, MEGABYTE);
    private final List<Type> types;
    private final Supplier<Consumer<Page>> exchangerSupplier;

    private final List<LocalExchangeSource> sources;

    private final LocalExchangeMemoryManager memoryManager;

    @GuardedBy("this")
    private boolean allSourcesFinished;

    @GuardedBy("this")
    private boolean noMoreSinkFactories;

    @GuardedBy("this")
    private final List<LocalExchangeSinkFactory> allSinkFactories;

    @GuardedBy("this")
    private final Set<LocalExchangeSinkFactory> openSinkFactories = new HashSet<>();

    @GuardedBy("this")
    private final Set<LocalExchangeSink> sinks = new HashSet<>();

    // TODO: remove test usage
    public LocalExchange(
            int sinkFactoryCount,
            PartitioningHandle partitioning,
            int defaultConcurrency,
            List<? extends Type> types,
            List<Integer> partitionChannels,
            Optional<Integer> partitionHashChannel)
    {
        this(sinkFactoryCount, computeBufferCount(partitioning, defaultConcurrency, partitionChannels), partitioning, defaultConcurrency, types, partitionChannels, partitionHashChannel, DEFAULT_MAX_BUFFERED_BYTES);
    }

    public LocalExchange(
            int sinkFactoryCount,
            int bufferCount,
            PartitioningHandle partitioning,
            int defaultConcurrency,
            List<? extends Type> types,
            List<Integer> partitionChannels,
            Optional<Integer> partitionHashChannel,
            DataSize maxBufferedBytes)
    {
        this.allSinkFactories = Stream.generate(() -> new LocalExchangeSinkFactory(LocalExchange.this))
                .limit(sinkFactoryCount)
                .collect(toImmutableList());
        openSinkFactories.addAll(allSinkFactories);
        noMoreSinkFactories();
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));

        ImmutableList.Builder<LocalExchangeSource> sources = ImmutableList.builder();
        for (int i = 0; i < bufferCount; i++) {
            sources.add(new LocalExchangeSource(types, source -> checkAllSourcesFinished()));
        }
        this.sources = sources.build();

        List<Consumer<PageReference>> buffers = this.sources.stream()
                .map(buffer -> (Consumer<PageReference>) buffer::addPage)
                .collect(toImmutableList());

        this.memoryManager = new LocalExchangeMemoryManager(maxBufferedBytes.toBytes());
        if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            exchangerSupplier = () -> new BroadcastExchanger(buffers, memoryManager::updateMemoryUsage);
        }
        else if (partitioning.equals(FIXED_BROADCAST_DISTRIBUTION)) {
            exchangerSupplier = () -> new BroadcastExchanger(buffers, memoryManager::updateMemoryUsage);
        }
        else if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            exchangerSupplier = () -> new RandomExchanger(buffers, memoryManager::updateMemoryUsage);
        }
        else if (partitioning.equals(FIXED_HASH_DISTRIBUTION)) {
            exchangerSupplier = () -> new PartitioningExchanger(buffers, memoryManager::updateMemoryUsage, types, partitionChannels, partitionHashChannel);
        }
        else {
            throw new IllegalArgumentException("Unsupported local exchange partitioning " + partitioning);
        }
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public int getBufferCount()
    {
        return sources.size();
    }

    public long getBufferedBytes()
    {
        return memoryManager.getBufferedBytes();
    }

    // TODO: remove test usages
    public synchronized LocalExchangeSinkFactory createSinkFactory()
    {
        checkState(!noMoreSinkFactories, "No more sink factories already set");
        LocalExchangeSinkFactory newFactory = new LocalExchangeSinkFactory(this);
        openSinkFactories.add(newFactory);
        return newFactory;
    }

    public synchronized LocalExchangeSinkFactory getSinkFactory(LocalExchangeSinkFactoryId id)
    {
        return allSinkFactories.get(id.id);
    }

    private int nextSourceIndex;

    public synchronized LocalExchangeSource getNextSource()
    {
        checkState(nextSourceIndex < sources.size(), "All operators already created");
        LocalExchangeSource result = sources.get(nextSourceIndex);
        nextSourceIndex++;
        return result;
    }

    public LocalExchangeSource getSource(int partitionIndex)
    {
        return sources.get(partitionIndex);
    }

    private void checkAllSourcesFinished()
    {
        checkNotHoldsLock(this);

        if (!sources.stream().allMatch(LocalExchangeSource::isFinished)) {
            return;
        }

        // all sources are finished, so finish the sinks
        ImmutableList<LocalExchangeSink> openSinks;
        synchronized (this) {
            allSourcesFinished = true;

            openSinks = ImmutableList.copyOf(sinks);
            sinks.clear();
        }

        // since all sources are finished there is no reason to allow new pages to be added
        // this can happen with a limit query
        openSinks.forEach(LocalExchangeSink::finish);
        checkAllSinksComplete();
    }

    private LocalExchangeSink createSink(LocalExchangeSinkFactory factory)
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            checkState(openSinkFactories.contains(factory), "Factory is already closed");

            if (allSourcesFinished) {
                // all sources have completed so return a sink that is already finished
                return finishedLocalExchangeSink(types, memoryManager);
            }

            // Note: exchanger can be stateful so create a new one for each sink
            Consumer<Page> exchanger = exchangerSupplier.get();
            LocalExchangeSink sink = new LocalExchangeSink(types, exchanger, memoryManager, this::sinkFinished);
            sinks.add(sink);
            return sink;
        }
    }

    private void sinkFinished(LocalExchangeSink sink)
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            sinks.remove(sink);
        }
        checkAllSinksComplete();
    }

    private void noMoreSinkFactories()
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            noMoreSinkFactories = true;
        }
        checkAllSinksComplete();
    }

    private void sinkFactoryClosed(LocalExchangeSinkFactory sinkFactory)
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            openSinkFactories.remove(sinkFactory);
        }
        checkAllSinksComplete();
    }

    private void checkAllSinksComplete()
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            if (!noMoreSinkFactories || !openSinkFactories.isEmpty() || !sinks.isEmpty()) {
                return;
            }
        }

        sources.forEach(LocalExchangeSource::finish);
        memoryManager.setNoBlockOnFull();
    }

    private static void checkNotHoldsLock(Object lock)
    {
        checkState(!Thread.holdsLock(lock), "Can not execute this method while holding a lock");
    }

    public static class LocalExchangeFactory
    {
        private final PartitioningHandle partitioning;
        private final int defaultConcurrency;
        private final List<? extends Type> types;
        private final List<Integer> partitionChannels;
        private final Optional<Integer> partitionHashChannel;
        private final DataSize maxBufferedBytes;
        private final int bufferCount;

        private boolean noMoreSinkFactories;
        // The number of total sink factories are tracked at planning time
        // so that the exact number of sink factory is known by the time execution starts.
        private int numSinkFactories;

        private ConcurrentMap<DriverGroupId, LocalExchange> localExchangeMap = new ConcurrentHashMap<>();
        private List<LocalExchangeSinkFactoryId> closedSinkFactories = new ArrayList<>();

        public LocalExchangeFactory(
                PartitioningHandle partitioning,
                int defaultConcurrency,
                List<? extends Type> types,
                List<Integer> partitionChannels,
                Optional<Integer> partitionHashChannel)
        {
            this(partitioning, defaultConcurrency, types, partitionChannels, partitionHashChannel, DEFAULT_MAX_BUFFERED_BYTES);
        }

        public LocalExchangeFactory(
                PartitioningHandle partitioning,
                int defaultConcurrency,
                List<? extends Type> types,
                List<Integer> partitionChannels,
                Optional<Integer> partitionHashChannel,
                DataSize maxBufferedBytes)
        {
            this.partitioning = requireNonNull(partitioning, "partitioning is null");
            this.defaultConcurrency = defaultConcurrency;
            this.types = requireNonNull(types, "types is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitioningChannels is null");
            this.partitionHashChannel = requireNonNull(partitionHashChannel, "partitionHashChannel is null");
            this.maxBufferedBytes = requireNonNull(maxBufferedBytes, "maxBufferedBytes is null");

            this.bufferCount = computeBufferCount(partitioning, defaultConcurrency, partitionChannels);
        }

        public synchronized LocalExchangeSinkFactoryId newSinkFactoryId()
        {
            checkState(!noMoreSinkFactories);
            LocalExchangeSinkFactoryId result = new LocalExchangeSinkFactoryId(numSinkFactories);
            numSinkFactories++;
            return result;
        }

        public synchronized void noMoreSinkFactories()
        {
            noMoreSinkFactories = true;
        }

        public List<? extends Type> getTypes()
        {
            return types;
        }

        public int getBufferCount()
        {
            return bufferCount;
        }

        // TODO! remove usage of this method in tests
        public LocalExchange createLocalExchange()
        {
            return new LocalExchange(numSinkFactories, bufferCount, partitioning, defaultConcurrency, types, partitionChannels, partitionHashChannel, maxBufferedBytes);
        }

        public synchronized LocalExchange getLocalExchange(DriverGroupId driverGroupId)
        {
            AtomicBoolean isNew = new AtomicBoolean();
            LocalExchange result = localExchangeMap.computeIfAbsent(driverGroupId, ignored -> {
                isNew.set(true);
                LocalExchange localExchange = createLocalExchange();
                for (LocalExchangeSinkFactoryId closedSinkFactoryId : closedSinkFactories) {
                    localExchange.getSinkFactory(closedSinkFactoryId).close();
                }
                return localExchange;
            });
            if (isNew.get() && listener != null) {
                listener.accept(driverGroupId);
            }
            return result;
        }

        public synchronized void closeSinks(LocalExchangeSinkFactoryId sinkFactoryId)
        {
            closedSinkFactories.add(sinkFactoryId);
            for (LocalExchange localExchange : localExchangeMap.values()) {
                localExchange.getSinkFactory(sinkFactoryId).close();
            }
        }

        // TODO: remove this hack! this should be possible. SqlTaskExecution could create DriverSplitRunner for all intermediate
        // pipelines when a new driver group is encountered.
        private Consumer<DriverGroupId> listener;
        public synchronized void setNewLocalExchangeListener(Consumer<DriverGroupId> listener)
        {
            requireNonNull(listener, "listener is null");
            checkState(this.listener == null, "listener already set");
            this.listener = listener;
        }
    }

    private static int computeBufferCount(PartitioningHandle partitioning, int defaultConcurrency, List<Integer> partitionChannels)
    {
        int bufferCount;
        if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            bufferCount = 1;
            checkArgument(partitionChannels.isEmpty(), "Gather exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_BROADCAST_DISTRIBUTION)) {
            bufferCount = defaultConcurrency;
            checkArgument(partitionChannels.isEmpty(), "Broadcast exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            bufferCount = defaultConcurrency;
            checkArgument(partitionChannels.isEmpty(), "Random exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_HASH_DISTRIBUTION)) {
            bufferCount = defaultConcurrency;
            checkArgument(!partitionChannels.isEmpty(), "Partitioned exchange must have partition channels");
        }
        else {
            throw new IllegalArgumentException("Unsupported local exchange partitioning " + partitioning);
        }
        return bufferCount;
    }

    public static class LocalExchangeSinkFactoryId
    {
        private final int id;

        public LocalExchangeSinkFactoryId(int id)
        {
            this.id = id;
        }
    }

    // Sink factory is entirely a pass thought to LocalExchange.
    // This class only exists as a separate entity to deal with the complex lifecycle caused
    // by operator factories (e.g., duplicate and noMoreSinkFactories).
    @ThreadSafe
    public static class LocalExchangeSinkFactory
            implements Closeable
    {
        private final LocalExchange exchange;

        private LocalExchangeSinkFactory(LocalExchange exchange)
        {
            this.exchange = requireNonNull(exchange, "exchange is null");
        }

        public List<Type> getTypes()
        {
            return exchange.getTypes();
        }

        public LocalExchangeSink createSink()
        {
            return exchange.createSink(this);
        }

        public LocalExchangeSinkFactory duplicate()
        {
            return exchange.createSinkFactory();
        }

        @Override
        public void close()
        {
            exchange.sinkFactoryClosed(this);
        }

        public void noMoreSinkFactories()
        {
            exchange.noMoreSinkFactories();
        }
    }
}
