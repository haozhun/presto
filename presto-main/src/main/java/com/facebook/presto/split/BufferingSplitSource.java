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
package com.facebook.presto.split;

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.execution.DriverGroupId;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class BufferingSplitSource
        implements SplitSource
{
    private final int bufferSize;
    private final SplitSource source;

    public BufferingSplitSource(SplitSource source, int bufferSize)
    {
        this.source = requireNonNull(source, "source is null");
        this.bufferSize = bufferSize;
    }

    @Override
    public ConnectorId getConnectorId()
    {
        return source.getConnectorId();
    }

    @Override
    public ConnectorTransactionHandle getTransactionHandle()
    {
        return source.getTransactionHandle();
    }

    @Override
    public ListenableFuture<List<Split>> getNextBatch(int maxSize)
    {
        checkArgument(maxSize > 0, "Cannot fetch a batch of zero size");
        return Futures.transform(getNextBatch(DriverGroupId.empty(), maxSize), SplitBatch::getSplits);
    }

    @Override
    public ListenableFuture<SplitBatch> getNextBatch(DriverGroupId driverGroupId, int maxSize)
    {
        checkArgument(maxSize > 0, "Cannot fetch a batch of zero size");
        FetchSplitsResult fetchSplitsResult = new FetchSplitsResult();
        ListenableFuture<?> future = fetchSplits(Math.min(bufferSize, maxSize), maxSize, driverGroupId, fetchSplitsResult);
        return Futures.transform(future, ignored -> fetchSplitsResult.toSplitBatch());
    }

    private ListenableFuture<?> fetchSplits(int min, int max, DriverGroupId driverGroupId, FetchSplitsResult result)
    {
        checkArgument(min <= max, "Min splits greater than max splits");
        if (source.isFinished() || result.splitSize() >= min) {
            return immediateFuture(null);
        }
        ListenableFuture<SplitBatch> future = source.getNextBatch(driverGroupId, max - result.splitSize());
        return Futures.transformAsync(future, splits -> {
            result.update(splits);
            if (splits.isNoMoreSplits()) {
                return immediateFuture(null);
            }
            return fetchSplits(min, max, driverGroupId, result);
        });
    }

    @Override
    public void close()
    {
        source.close();
    }

    @Override
    public boolean isFinished()
    {
        return source.isFinished();
    }

    private static class FetchSplitsResult
    {
        private final List<Split> splits = new ArrayList<>();
        private boolean noMoreSplits;

        public synchronized void update(SplitBatch splitBatch)
        {
            splits.addAll(splitBatch.getSplits());
            if (splitBatch.isNoMoreSplits()) {
                noMoreSplits = true;
            }
        }

        public synchronized int splitSize()
        {
            return splits.size();
        }

        public synchronized SplitBatch toSplitBatch()
        {
            return new SplitBatch(splits, noMoreSplits);
        }
    }
}
