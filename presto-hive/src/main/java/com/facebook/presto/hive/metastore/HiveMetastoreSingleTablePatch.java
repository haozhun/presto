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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class HiveMetastoreSingleTablePatch
{
    private final SchemaTableName schemaTableName;
    private final Optional<Table> table;
    private final Optional<Map<List<String>, SemiTransactionalHiveMetastore.Action<Partition>>> partitionActions;

    public HiveMetastoreSingleTablePatch(
            SchemaTableName schemaTableName,
            Optional<Table> table,
            Optional<Map<List<String>, SemiTransactionalHiveMetastore.Action<Partition>>> partitionActions)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.table = requireNonNull(table, "table is null");
        this.partitionActions = requireNonNull(partitionActions, "partitionActions is null");
    }

    @JsonCreator
    public static HiveMetastoreSingleTablePatch deserialize(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("table") Optional<Table> table,
            @JsonProperty("partitionActions") Optional<List<JsonSerializableEntry<List<String>, SemiTransactionalHiveMetastore.Action<Partition>>>> partitionActions)
    {
        requireNonNull(partitionActions, "partitionActions is null");
        return new HiveMetastoreSingleTablePatch(schemaTableName, table, partitionActions.map(JsonSerializableEntry::toMap));
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public Optional<Table> getTable()
    {
        return table;
    }

    @JsonProperty("partitionActions")
    public Optional<List<JsonSerializableEntry<List<String>, SemiTransactionalHiveMetastore.Action<Partition>>>> getJsonSerializablePartitionActions()
    {
        return partitionActions.map(JsonSerializableEntry::fromMap);
    }

    public Optional<Map<List<String>, SemiTransactionalHiveMetastore.Action<Partition>>> getPartitionActions()
    {
        return partitionActions;
    }

    public static class JsonSerializableEntry<K, V>
    {
        private final K key;
        private final V value;

        @JsonCreator
        public JsonSerializableEntry(@JsonProperty("key") K key, @JsonProperty("value") V value)
        {
            this.key = key;
            this.value = value;
        }

        @JsonProperty
        public K getKey()
        {
            return key;
        }

        @JsonProperty
        public V getValue()
        {
            return value;
        }

        public static <K, V> List<JsonSerializableEntry<K, V>> fromMap(Map<K, V> map)
        {
            return map.entrySet().stream()
                    .map(entry -> new JsonSerializableEntry<>(entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList());
        }

        public static <K, V> Map<K, V> toMap(List<JsonSerializableEntry<K, V>> list)
        {
            return list.stream()
                    .collect(Collectors.toMap(JsonSerializableEntry::getKey, JsonSerializableEntry::getValue));
        }
    }
}
