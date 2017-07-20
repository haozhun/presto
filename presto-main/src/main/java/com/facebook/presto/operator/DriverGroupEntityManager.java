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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class DriverGroupEntityManager<T>
{
    private final Function<DriverGroupId, T> function;
    private final Map<DriverGroupId, T> map = new ConcurrentHashMap<>();

    public DriverGroupEntityManager(Function<DriverGroupId, T> function)
    {
        this.function = requireNonNull(function, "function is null");
    }

    public T forDriverGroup(DriverGroupId driverGroupId)
    {
        return map.computeIfAbsent(driverGroupId, function);
    }

    public void forEachDriverGroup(Consumer<DriverGroupId> driverGroupCallback)
    {
        // TODO: simplify
        for (Map.Entry<DriverGroupId, T> entry : map.entrySet()) {
            driverGroupCallback.accept(entry.getKey());
        }
    }

//    public void forEachDriverGroup(BiConsumer<DriverGroupId, T> driverGroupCallback)
//    {
//        for (Map.Entry<DriverGroupId, T> entry : map.entrySet()) {
//            driverGroupCallback.accept(entry.getKey(), entry.getValue());
//        }
//    }

    public static <T> DriverGroupEntityManager<T> testOnly(T object)
    {
        return new DriverGroupEntityManager<>(ignored -> object);
    }

    public static class LookupSourceFactoryManager
    {
        private final DriverGroupEntityManager<LookupSourceFactory> manager;
        private final List<Type> outputTypes;

        public LookupSourceFactoryManager(List<Type> types, List<Type> outputTypes, List<Type> hashChannelTypes, int partitionCount, Map<Symbol, Integer> layout, boolean outer)
        {
            this(
                    DriverGroupEntityManager.testOnly(new PartitionedLookupSourceFactory(types, outputTypes, hashChannelTypes, partitionCount, layout, outer)),
                    outputTypes);
        }

        public LookupSourceFactoryManager(DriverGroupEntityManager<LookupSourceFactory> manager, List<Type> outputTypes)
        {
            this.manager = manager;
            this.outputTypes = outputTypes;
        }

        public List<Type> getBuildOutputTypes()
        {
            return outputTypes;
        }

        public LookupSourceFactory forDriverGroup(DriverGroupId driverGroupId)
        {
            return manager.forDriverGroup(driverGroupId);
        }

        public void forEachDriverGroup(Consumer<DriverGroupId> driverGroupCallback)
        {
            manager.forEachDriverGroup(driverGroupCallback);
        }
    }
}
