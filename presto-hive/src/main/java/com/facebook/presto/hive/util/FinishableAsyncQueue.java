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

import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@ThreadSafe
public class FinishableAsyncQueue<T>
{
    private static final CompletableFuture<?> COMPLETED_FUTURE_OF_NULL = CompletableFuture.completedFuture(null);

    private final AsyncQueue<T> queue;
    private final AtomicBoolean finished = new AtomicBoolean(false);

    private final CompletableFuture<List<T>> completedFutureOfEmptyList = CompletableFuture.completedFuture(ImmutableList.of());

    public FinishableAsyncQueue(int softMaxQueueSize)
    {
        queue = new AsyncQueue<>(softMaxQueueSize);
    }

    /**
     * @return upper bound of number of elements in queue.
     */
    public int size()
    {
        return queue.size();
    }

    /**
     * After calling this function, subsequent calls to {@link #offer(Object)} will be ignored. In addition,
     * all futures returned previously will be immediately completed. Subsequent calls to {@link #offer(Object)}
     * and {@link #getBatchAsync(int)} will always return a completed future. Ongoing calls to
     * {@link #offer(Object)} may or may not be ignored.
     */
    public void finish()
    {
        if (finished.compareAndSet(false, true)) {
            // It must be guaranteed that no one will become blocked once execution hits this point.
            queue.signalAll();
        }
    }

    public boolean isFinished()
    {
        // When size becomes 0, it means all elements that were offered before finish is called has been get.
        return finished.get() && size() == 0;
    }

    public CompletableFuture<?> offer(T element)
    {
        if (finished.get()) {
            return COMPLETED_FUTURE_OF_NULL;
        }
        CompletableFuture<?> result = queue.offer(element);
        // If `finish` is called before this point, if will evaluate to true and a completed future will be returned.
        // If `finish` is called after this point, the previously returned future will be completed.
        // As a result, the guarantee requirement in the comment of `finish` is satisfied for `offer`.
        if (finished.get()) {
            return COMPLETED_FUTURE_OF_NULL;
        }
        return result;
    }

    public CompletableFuture<List<T>> getBatchAsync(int maxSize)
    {
        CompletableFuture<List<T>> result = queue.getBatchAsync(maxSize);
        if (finished.get()) {
            if (!result.isDone()) {
                // Items will not be added to the queue after finished is set. However, there might be items being added while finished got set.
                // This isDone check can potentially lead to loss of such items. But it doesn't matter.
                return completedFutureOfEmptyList;
            }
        }
        return result;
    }
}
