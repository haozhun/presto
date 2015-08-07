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
package com.facebook.presto.hive.util;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class AsyncQueue<T>
{
    public static final CompletableFuture<?> COMPLETED_FUTURE = CompletableFuture.completedFuture(null);

    private final int softMaxQueueSize;

    private final BlockingQueue<T> elements = new LinkedBlockingQueue<>();
    // Upper bound of number of elements in `elements`. Increment happens before add. Decrement happens after removal.
    // The order is chosen to guarantee that this variable never becomes negative.
    private final AtomicInteger queueSize = new AtomicInteger();
    private final AtomicReference<CompletableFuture<?>> offerResubmissionFutureReference = new AtomicReference<>(new CompletableFuture<>());
    private final AtomicReference<CompletableFuture<?>> fetchResubmissionFutureReference = new AtomicReference<>(new CompletableFuture<>());

    public AsyncQueue(int softMaxQueueSize)
    {
        this.softMaxQueueSize = softMaxQueueSize;
    }

    /**
     * @return upper bound of number of elements in queue
     */
    public int size()
    {
        return queueSize.get();
    }

    public CompletableFuture<?> offer(T element)
    {
        requireNonNull(element);

        // Make sure to grab the futures before adding to the queue.
        // This guarantees that the future will be current (or already signaled), but never newer.
        CompletableFuture<?> offerResubmissionFuture = offerResubmissionFutureReference.get();

        int newSize = queueSize.incrementAndGet();
        elements.add(element);
        if (newSize == 1) {
            signalFetchResubmissionAndReset();
        }
        return newSize < softMaxQueueSize ? COMPLETED_FUTURE : offerResubmissionFuture;
    }

    private int drainTo(Collection<T> collection, int maxSize)
    {
        int drained = elements.drainTo(collection, maxSize);
        if (drained > 0) {
            reduceQueueCount(drained);
        }
        return drained;
    }

    private List<T> getBatch(int maxSize)
    {
        List<T> list = new ArrayList<>(maxSize);
        drainTo(list, maxSize);
        return list;
    }

    public CompletableFuture<List<T>> getBatchAsync(int maxSize)
    {
        // Grab the future first.
        CompletableFuture<?> future = fetchResubmissionFutureReference.get();

        List<T> list = getBatch(maxSize);
        if (list.isEmpty()) {
            return future.thenApply(x -> getBatch(maxSize));
        }
        else {
            return CompletableFuture.completedFuture(list);
        }
    }

    private void reduceQueueCount(int reduceBy)
    {
        int newSize = queueSize.addAndGet(-reduceBy);
        verify(newSize >= 0);
        // This checks that the queue size changed from above threshold to below. Therefore, writers shall be notified.
        if (newSize + reduceBy >= softMaxQueueSize && newSize < softMaxQueueSize) {
            signalOfferResubmissionAndReset();
        }
    }

    private void signalOfferResubmissionAndReset()
    {
        offerResubmissionFutureReference.getAndSet(new CompletableFuture<>()).complete(null);
    }

    private void signalFetchResubmissionAndReset()
    {
        fetchResubmissionFutureReference.getAndSet(new CompletableFuture<>()).complete(null);
    }

    protected void signalAll()
    {
        signalOfferResubmissionAndReset();
        signalFetchResubmissionAndReset();
    }
}
