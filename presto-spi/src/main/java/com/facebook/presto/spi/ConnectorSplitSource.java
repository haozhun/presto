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
package com.facebook.presto.spi;

import java.io.Closeable;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public interface ConnectorSplitSource
        extends Closeable
{
    CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize);

    default CompletableFuture<ConnectorSplitBatch> getNextBatch(OptionalInt driverGroupId, int maxSize)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    void close();

    boolean isFinished();

    class ConnectorSplitBatch
    {
        private final List<ConnectorSplit> splits;
        private final boolean noMoreSplits;

        public ConnectorSplitBatch(List<ConnectorSplit> splits, boolean noMoreSplits)
        {
            this.splits = requireNonNull(splits, "splits is null");
            this.noMoreSplits = noMoreSplits;
        }

        public List<ConnectorSplit> getSplits()
        {
            return splits;
        }

        public boolean isNoMoreSplits()
        {
            return noMoreSplits;
        }
    }
}
