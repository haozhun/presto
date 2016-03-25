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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class Partition
{
    private final String databaseName;
    private final String tableName;
    private final List<String> values;
    private final Storage storage;
    private final List<FieldSchema> columns;
    private final Map<String, String> parameters;

    @JsonCreator
    public Partition(
            @JsonProperty("databaseName") String databaseName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("values") List<String> values,
            @JsonProperty("storage") Storage storage,
            @JsonProperty("columns") List<FieldSchema> columns,
            @JsonProperty("parameters") Map<String, String> parameters)
    {
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.values = ImmutableList.copyOf(requireNonNull(values, "values is null"));
        this.storage = requireNonNull(storage, "storage is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.parameters = ImmutableMap.copyOf(requireNonNull(parameters, "parameters is null"));
    }

    @JsonProperty
    public String getDatabaseName()
    {
        return databaseName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<String> getValues()
    {
        return values;
    }

    @JsonProperty
    public Storage getStorage()
    {
        return storage;
    }

    @JsonProperty
    public List<FieldSchema> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Map<String, String> getParameters()
    {
        return parameters;
    }

    public static class Builder
    {
        private final Storage.Builder storageBuilder = new Storage.Builder();
        private String databaseName;
        private String tableName;
        private List<String> values;
        private List<FieldSchema> columns;
        private Map<String, String> parameters = ImmutableMap.of();

        public Builder setDatabaseName(String databaseName)
        {
            this.databaseName = databaseName;
            return this;
        }

        public Builder setTableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public Builder setValues(List<String> values)
        {
            this.values = values;
            return this;
        }

        public Storage.Builder getStorageBuilder()
        {
            return storageBuilder;
        }

        public Builder setColumns(List<FieldSchema> columns)
        {
            this.columns = columns;
            return this;
        }

        public Builder setParameters(Map<String, String> parameters)
        {
            this.parameters = parameters;
            return this;
        }

        public Partition build()
        {
            return new Partition(databaseName, tableName, values, storageBuilder.build(), columns, parameters);
        }
    }
}
