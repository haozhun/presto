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

import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.PartitionAlreadyExistsException;
import com.facebook.presto.hive.PartitionNotFoundException;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.common.FileUtils.unescapePathName;

public class SemiTransactionalHiveMetastore
        implements ExtendedHiveMetastore
{
    //TODO: does this class need to be thread-safe?

    private static final Logger log = Logger.get(SemiTransactionalHiveMetastore.class);
    private static final int PARTITION_COMMIT_BATCH_SIZE = 8;
    private static final String PARTITION_VALUE_WILDCARD = "";

    private final ExtendedHiveMetastore delegate;

    private final Map<SchemaTableName, Action<TableAndPrivilege>> tableActions = new HashMap<>();
    private final Map<SchemaTableName, Map<List<String>, Action<Partition>>> partitionActions = new HashMap<>();

    public SemiTransactionalHiveMetastore(ExtendedHiveMetastore delegate)
    {
        this.delegate = delegate;
    }

    public void rollback()
    {
    }

    public void commit()
    {
        for (Map.Entry<SchemaTableName, Action<TableAndPrivilege>> entry : tableActions.entrySet()) {
            SchemaTableName schemaTableName = entry.getKey();
            Action<TableAndPrivilege> action = entry.getValue();
            switch (action.getType()) {
                case DROP:
                    delegate.dropTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
                    break;
                case ALTER:
                    throw new UnsupportedOperationException("not implemented yet");
                case ADD:
                    delegate.createTable(action.getData().getTable(), action.getData().getPrincipalPrivilegeSet());
                    break;
            }
        }
        for (Map.Entry<SchemaTableName, Map<List<String>, Action<Partition>>> tableEntry : partitionActions.entrySet()) {
            SchemaTableName schemaTableName = tableEntry.getKey();
            PartitionCommitter partitionCommitter = new PartitionCommitter(schemaTableName.getSchemaName(), schemaTableName.getTableName(), delegate, PARTITION_COMMIT_BATCH_SIZE);
            for (Map.Entry<List<String>, Action<Partition>> partitionEntry : tableEntry.getValue().entrySet()) {
                List<String> partitionValues = partitionEntry.getKey();
                Action<Partition> action = partitionEntry.getValue();
                switch (action.getType()) {
                    case DROP:
                        delegate.dropPartition(schemaTableName.getSchemaName(), schemaTableName.getTableName(), partitionValues);
                        break;
                    case ALTER:
                        delegate.getPartition(schemaTableName.getSchemaName(), schemaTableName.getTableName(), action.getData().getValues());
                        delegate.alterPartition(schemaTableName.getSchemaName(), schemaTableName.getTableName(), action.getData());
                        break;
                    case ADD:
                        partitionCommitter.addPartition(action.getData());
                        break;
                }
            }
            partitionCommitter.flush();
        }
    }

    public HiveMetastoreSingleTablePatch generatePartitionPatch(SchemaTableName schemaTableName)
    {
        Optional<Table> table = getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        if (!table.isPresent()) {
            return new HiveMetastoreSingleTablePatch(schemaTableName, Optional.empty(), Optional.empty());
        }
        return new HiveMetastoreSingleTablePatch(
                schemaTableName,
                table,
                Optional.of(partitionActions.computeIfAbsent(schemaTableName, k -> new HashMap<>())));
    }

    @Override
    public void createTable(Table table, PrincipalPrivilegeSet principalPrivilegeSet)
    {
        SchemaTableName schemaTableName = toSchemaTableName(table);
        Action<TableAndPrivilege> oldTableAction = tableActions.get(schemaTableName);
        TableAndPrivilege tableAndPrivilege = new TableAndPrivilege(table, principalPrivilegeSet);
        if (oldTableAction == null) {
            tableActions.put(schemaTableName, new Action<>(ActionType.ADD, tableAndPrivilege));
            return;
        }
        switch (oldTableAction.getType()) {
            case DROP:
                tableActions.put(schemaTableName, new Action<>(ActionType.ALTER, tableAndPrivilege));
                break;
            case ADD:
            case ALTER:
                throw new TableAlreadyExistsException(schemaTableName);
        }
    }

    @Override
    public void dropTable(String databaseName, String tableName)
    {
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        Action<TableAndPrivilege> oldTableAction = tableActions.get(schemaTableName);
        if (oldTableAction == null || oldTableAction.getType() == ActionType.ALTER) {
            tableActions.put(schemaTableName, new Action<>(ActionType.DROP, null));
            return;
        }
        switch (oldTableAction.getType()) {
            case DROP:
                throw new TableNotFoundException(schemaTableName);
            case ADD:
                //TODO
                throw new UnsupportedOperationException("dropping a table added in the same transaction is not supported");
            case ALTER:
                tableActions.put(schemaTableName, new Action<>(ActionType.DROP, null));
                break;
        }
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTable(String databaseName, String tableName, Table table, PrincipalPrivilegeSet principalPrivilegeSet)
    {
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        Action<TableAndPrivilege> oldTableAction = tableActions.get(schemaTableName);
        if (oldTableAction == null || oldTableAction.getType() == ActionType.ALTER) {
            tableActions.put(schemaTableName, new Action<>(ActionType.DROP, null));
            return;
        }
        switch (oldTableAction.getType()) {
            case DROP:
                throw new TableNotFoundException(schemaTableName);
            case ADD:
                tableActions.put(schemaTableName, new Action<>(ActionType.ADD, new TableAndPrivilege(table, principalPrivilegeSet)));
                break;
            case ALTER:
                tableActions.put(schemaTableName, new Action<>(ActionType.ALTER, null));
                break;
        }
    }

    @Override
    public void flushCache()
    {
    }

    @Override
    public List<String> getAllDatabases()
    {
        return delegate.getAllDatabases();
    }

    @Override
    public Optional<List<String>> getAllTables(String databaseName)
    {
        if (!tableActions.isEmpty()) {
            throw new UnsupportedOperationException("listing all tables after adding/dropping/altering tables/views in a transaction is not supported");
        }
        return delegate.getAllTables(databaseName);
    }

    @Override
    public Optional<List<String>> getAllViews(String databaseName)
    {
        if (!tableActions.isEmpty()) {
            throw new UnsupportedOperationException("listing all tables after adding/dropping/altering tables/views in a transaction is not supported");
        }
        return delegate.getAllViews(databaseName);
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return delegate.getDatabase(databaseName);
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<Partition> partitions)
    {
        for (Partition partition : partitions) {
            addPartition(databaseName, tableName, partition);
        }
    }

    private void addPartition(String databaseName, String tableName, Partition partition)
    {
        Map<List<String>, Action<Partition>> partitionActionsOfTable = partitionActions.computeIfAbsent(new SchemaTableName(databaseName, tableName), k -> new HashMap<>());
        Action<Partition> oldPartitionAction = partitionActionsOfTable.get(partition.getValues());
        if (oldPartitionAction == null) {
            partitionActionsOfTable.put(partition.getValues(), new Action<>(ActionType.ADD, partition));
            return;
        }
        switch (oldPartitionAction.getType()) {
            case DROP:
                partitionActionsOfTable.put(partition.getValues(), new Action<>(ActionType.ALTER, partition));
                break;
            case ADD:
            case ALTER:
                throw new PartitionAlreadyExistsException(new SchemaTableName(databaseName, tableName), Optional.of(partition.getValues()));
        }
    }

    @Override
    public void alterPartition(String databaseName, String tableName, Partition partition)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts)
    {
        Map<List<String>, Action<Partition>> partitionActionsOfTable = partitionActions.computeIfAbsent(new SchemaTableName(databaseName, tableName), k -> new HashMap<>());
        Action<Partition> oldPartitionAction = partitionActionsOfTable.get(parts);
        if (oldPartitionAction == null) {
            partitionActionsOfTable.put(parts, new Action<>(ActionType.DROP, null));
            return;
        }
        switch (oldPartitionAction.getType()) {
            case DROP:
                throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), parts);
            case ADD:
            case ALTER:
                throw new PrestoException(
                        NOT_SUPPORTED,
                        format("dropping a partition added in the same transaction is not supported: %s %s %s", databaseName, tableName, parts));
        }
    }

    @Override
    public void dropPartitionByName(String databaseName, String tableName, String partitionName)
    {
        // TODO: remove the caller?
        dropPartition(databaseName, tableName, toPartitionValues(partitionName));
    }

    @Override
    public Optional<List<String>> getPartitionNames(String databaseName, String tableName)
    {
        if (!tableActions.isEmpty()) {
            throw new UnsupportedOperationException("probing partitions after adding/dropping/altering tables/views in a transaction is not supported");
        }
        Optional<List<String>> partitionNames = delegate.getPartitionNames(databaseName, tableName);
        if (!partitionNames.isPresent()) {
            return Optional.empty();
        }
        Map<List<String>, Action<Partition>> partitionActionsOfTable = partitionActions.computeIfAbsent(new SchemaTableName(databaseName, tableName), k -> new HashMap<>());
        ImmutableList.Builder<String> resultBuilder = ImmutableList.builder();
        // alter/remove newly-altered/dropped partitions from underlying metastore
        for (String partitionName : partitionNames.get()) {
            List<String> partitionValues = toPartitionValues(partitionName);
            Action<Partition> partitionAction = partitionActionsOfTable.get(partitionValues);
            if (partitionAction == null) {
                resultBuilder.add(partitionName);
                continue;
            }
            switch (partitionAction.getType()) {
                case ADD:
                    throw new PartitionAlreadyExistsException(new SchemaTableName(databaseName, tableName), Optional.of(partitionValues));
                case DROP:
                    break;
                case ALTER:
                    resultBuilder.add(partitionName);
                    break;
            }
        }
        for (Action<Partition> partitionAction : partitionActionsOfTable.values()) {
            if (partitionAction.getType() == ActionType.ADD) {
                // TODO: How to turn into partition name? On the other hand, maybe don't use partition names in Presto at all?
                //resultBuilder.add(partitionAction.getData().getValues());
                throw new UnsupportedOperationException("TODO: hack is available, not implemented yet");
            }
        }
        return Optional.of(resultBuilder.build());
    }

    private static List<String> toPartitionValues(String partitionName)
    {
        // mimics Warheouse.makeValsFromName
        ImmutableList.Builder<String> resultBuilder = ImmutableList.builder();
        int start = 0;
        while (start < partitionName.length()) {
            while (start < partitionName.length() && partitionName.charAt(start) != '=') {
                start++;
            }
            start++;
            int end = start;
            while (end < partitionName.length() && partitionName.charAt(end) != '/') {
                end++;
            }
            resultBuilder.add(unescapePathName(partitionName.substring(start, end)));
            start = end + 1;
        }
        return resultBuilder.build();
    }

    @Override
    public Optional<List<String>> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts)
    {
        if (!tableActions.isEmpty()) {
            throw new UnsupportedOperationException("probing partitions after adding/dropping/altering tables/views in a transaction is not supported");
        }
        Optional<List<String>> partitionNames = delegate.getPartitionNamesByParts(databaseName, tableName, parts);
        if (!partitionNames.isPresent()) {
            return Optional.empty();
        }
        Map<List<String>, Action<Partition>> partitionActionsOfTable = partitionActions.computeIfAbsent(new SchemaTableName(databaseName, tableName), k -> new HashMap<>());
        ImmutableList.Builder<String> resultBuilder = ImmutableList.builder();
        // alter/remove newly-altered/dropped partitions from the results from underlying metastore
        for (String partitionName : partitionNames.get()) {
            List<String> partitionValues = toPartitionValues(partitionName);
            Action<Partition> partitionAction = partitionActionsOfTable.get(partitionValues);
            if (partitionAction == null) {
                resultBuilder.add(partitionName);
                continue;
            }
            switch (partitionAction.getType()) {
                case ADD:
                    throw new PartitionAlreadyExistsException(new SchemaTableName(databaseName, tableName), Optional.of(partitionValues));
                case DROP:
                    // do nothing
                    break;
                case ALTER:
                    resultBuilder.add(partitionName);
                    break;
            }
        }
        // add newly-added partitions to the results from underlying metastore
        for (Action<Partition> partitionAction : partitionActionsOfTable.values()) {
            if (partitionAction.getType() == ActionType.ADD) {
                List<String> values = partitionAction.getData().getValues();
                if (partitionValuesMatch(values, parts)) {
                    // TODO: How to turn into partition name? On the other hand, maybe don't use partition names in Presto at all?
                    //resultBuilder.add(values);
                    throw new UnsupportedOperationException("TODO: hack is available, not implemented yet");
                }
            }
        }
        return Optional.of(resultBuilder.build());
    }

    @Override
    public Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues)
    {
        Map<List<String>, Action<Partition>> partitionActionsOfTable = partitionActions.computeIfAbsent(new SchemaTableName(databaseName, tableName), k -> new HashMap<>());
        return getPartition(databaseName, tableName, partitionValues, partitionActionsOfTable, delegate);
    }

    static Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues, Map<List<String>, Action<Partition>> partitionActionsOfTable, ExtendedHiveMetastore delegate)
    {
        Action<Partition> partitionAction = partitionActionsOfTable.get(partitionValues);
        if (partitionAction == null) {
            return delegate.getPartition(databaseName, tableName, partitionValues);
        }
        else {
            switch (partitionAction.getType()) {
                case ADD:
                case ALTER:
                    return Optional.of(partitionAction.getData());
                case DROP:
                    return Optional.empty();
                default:
                    throw new UnsupportedOperationException("Unknown action type");
            }
        }
    }

    // TODO: The return type here is very weird
    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
    {
        Map<List<String>, Action<Partition>> partitionActionsOfTable = partitionActions.computeIfAbsent(new SchemaTableName(databaseName, tableName), k -> new HashMap<>());
        ImmutableList.Builder<String> partitionNamesToQuery = ImmutableList.builder();
        ImmutableMap.Builder<String, Optional<Partition>> resultBuilder = ImmutableMap.builder();
        for (String partitionName : partitionNames) {
            List<String> partitionValues = toPartitionValues(partitionName);
            Action<Partition> partitionAction = partitionActionsOfTable.get(partitionValues);
            if (partitionAction == null) {
                partitionNamesToQuery.add(partitionName);
            }
            else {
                switch (partitionAction.getType()) {
                    case ADD:
                    case ALTER:
                        resultBuilder.put(partitionName, Optional.of(partitionAction.getData()));
                    case DROP:
                        resultBuilder.put(partitionName, Optional.empty());
                    default:
                        throw new UnsupportedOperationException("Unknown action type");
                }
            }
        }
        Map<String, Optional<Partition>> delegateResult = delegate.getPartitionsByNames(databaseName, tableName, partitionNames);
        resultBuilder.putAll(delegateResult);
        return resultBuilder.build();
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        Action<TableAndPrivilege> tableAction = tableActions.get(new SchemaTableName(databaseName, tableName));
        if (tableAction == null) {
            return delegate.getTable(databaseName, tableName);
        }
        else {
            switch (tableAction.getType()) {
                case ADD:
                case ALTER:
                    return Optional.of(tableAction.getData().getTable());
                case DROP:
                    return Optional.empty();
                default:
                    throw new UnsupportedOperationException("Unknown action type");
            }
        }
    }

    @Override
    public Set<String> getRoles(String user)
    {
        return delegate.getRoles(user);
    }

    @Override
    public Set<HivePrivilegeInfo> getDatabasePrivileges(String user, String databaseName)
    {
        return delegate.getDatabasePrivileges(user, databaseName);
    }

    @Override
    public Set<HivePrivilegeInfo> getTablePrivileges(String user, String databaseName, String tableName)
    {
        return delegate.getTablePrivileges(user, databaseName, tableName);
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String grantee, Set<PrivilegeGrantInfo> privilegeGrantInfoSet)
    {
        delegate.grantTablePrivileges(databaseName, tableName, grantee, privilegeGrantInfoSet);
    }

    private boolean partitionValuesMatch(List<String> values, List<String> pattern)
    {
        checkArgument(values.size() != pattern.size());
        for (int i = 0; i < values.size(); i++) {
            if (PARTITION_VALUE_WILDCARD.equals(pattern.get(i))) {
                continue;
            }
            if (values.get(i).equals(pattern.get(i))) {
                return false;
            }
        }
        return true;
    }

    private SchemaTableName toSchemaTableName(Table table)
    {
        return new SchemaTableName(table.getDatabaseName(), table.getTableName());
    }

    private enum ActionType
    {
        DROP,
        ADD,
        ALTER
    }

    public static class Action<T>
    {
        private final ActionType type;
        private final T data;

        @JsonCreator
        public Action(@JsonProperty("type") ActionType type, @JsonProperty("data") T data)
        {
            this.type = requireNonNull(type, "type is null");
            if (type == ActionType.DROP) {
                checkArgument(data == null, "data is not null");
            }
            else {
                requireNonNull(data, "data is null");
            }
            this.data = data;
        }

        @JsonProperty
        public ActionType getType()
        {
            return type;
        }

        public T getData()
        {
            checkState(type != ActionType.DROP);
            return data;
        }

        @JsonProperty("data")
        public T getJsonSerializableData()
        {
            return data;
        }
    }

    private static class TableAndPrivilege
    {
        Table table;
        PrincipalPrivilegeSet principalPrivilegeSet;

        public TableAndPrivilege(Table table, PrincipalPrivilegeSet principalPrivilegeSet)
        {
            this.table = requireNonNull(table, "table is null");
            this.principalPrivilegeSet = requireNonNull(principalPrivilegeSet, "principalPrivilegeSet is null");
        }

        public Table getTable()
        {
            return table;
        }

        public PrincipalPrivilegeSet getPrincipalPrivilegeSet()
        {
            return principalPrivilegeSet;
        }
    }

    private static class PartitionIdentifier
    {
        private final String databaseName;
        private final String tableName;
        private final List<String> parts;

        public PartitionIdentifier(String databaseName, String tableName, List<String> parts)
        {
            this.databaseName = databaseName;
            this.tableName = tableName;
            this.parts = parts;
        }

        public String getDatabaseName()
        {
            return databaseName;
        }

        public String getTableName()
        {
            return tableName;
        }

        public List<String> getParts()
        {
            return parts;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PartitionIdentifier that = (PartitionIdentifier) o;
            return Objects.equals(databaseName, that.databaseName) &&
                    Objects.equals(tableName, that.tableName) &&
                    Objects.equals(parts, that.parts);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(databaseName, tableName, parts);
        }
    }

    private static class PartitionCommitter
    {
        private final String schemaName;
        private final String tableName;
        private final ExtendedHiveMetastore metastore;
        private final int batchSize;
        private final List<Partition> batch;
        private final List<Partition> createdPartitions = new ArrayList<>();

        public PartitionCommitter(String schemaName, String tableName, ExtendedHiveMetastore metastore, int batchSize)
        {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.metastore = metastore;
            this.batchSize = batchSize;
            this.batch = new ArrayList<>(batchSize);
        }

        public List<Partition> getCreatedPartitions()
        {
            return ImmutableList.copyOf(createdPartitions);
        }

        public void addPartition(Partition partition)
        {
            batch.add(partition);
            if (batch.size() >= batchSize) {
                addBatch();
            }
        }

        public void flush()
        {
            if (!batch.isEmpty()) {
                addBatch();
            }
        }

        public void abort()
        {
            // drop created partitions
            for (Partition createdPartition : getCreatedPartitions()) {
                try {
                    metastore.dropPartition(schemaName, tableName, createdPartition.getValues());
                }
                catch (Exception e) {
                    log.error(e, "Error rolling back new partition '%s' in table '%s.%s", createdPartition.getValues(), schemaName, tableName);
                }
            }
        }

        private void addBatch()
        {
            metastore.addPartitions(schemaName, tableName, batch);
            createdPartitions.addAll(batch);
            batch.clear();
        }
    }
}
