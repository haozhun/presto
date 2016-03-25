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

import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore.Action;
import com.facebook.presto.spi.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveMetastoreWithPatch
        implements HiveMetastoreGetOnly
{
    private final ExtendedHiveMetastore delegate;
    private final SchemaTableName schemaTableName;
    private final Optional<Table> table;
    private final Optional<Map<List<String>, Action<Partition>>> partitionActions;

    public HiveMetastoreWithPatch(HiveMetastoreSingleTablePatch patch, ExtendedHiveMetastore delegate)
    {
        requireNonNull(patch, "patch is null");
        this.delegate = delegate;
        this.schemaTableName = patch.getSchemaTableName();
        this.table = patch.getTable();
        this.partitionActions = patch.getPartitionActions();
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        if (!databaseName.equals(schemaTableName.getSchemaName()) || !tableName.equals(schemaTableName.getTableName())) {
            throw new IllegalArgumentException(
                    format("Unexpected table name. Got: %s %s. Expected: %s %s", databaseName, tableName, schemaTableName.getSchemaName(), schemaTableName.getTableName()));
        }
        return table;
    }

    @Override
    public Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues)
    {
        if (!databaseName.equals(schemaTableName.getSchemaName()) || !tableName.equals(schemaTableName.getTableName())) {
            throw new IllegalArgumentException(
                    format("Unexpected table name. Got: %s %s. Expected: %s %s", databaseName, tableName, schemaTableName.getSchemaName(), schemaTableName.getTableName()));
        }
        if (!partitionActions.isPresent()) {
            throw new IllegalArgumentException(
                    format("Unexpected call to getPartition. Table name: %s %s", databaseName, tableName));
        }
        return SemiTransactionalHiveMetastore.getPartition(databaseName, tableName, partitionValues, partitionActions.get(), delegate);
    }
}
