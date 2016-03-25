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
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.weakref.jmx.Managed;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.USER;

public interface ExtendedHiveMetastore
{
    String DEFAULT_DATABASE_NAME = "default";

    @Managed
    void flushCache();

    void createTable(Table table, PrincipalPrivilegeSet principalPrivilegeSet);

    void dropTable(String databaseName, String tableName);

    /**
     * This should only be used if the semantic here is drop and add. Trying to
     * alter one field of a table object previously acquired from getTable is
     * probably not what you want.
     */
    void alterTable(String databaseName, String tableName, Table newTable, PrincipalPrivilegeSet principalPrivilegeSet);

    void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName);

    void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment);

    void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName);

    List<String> getAllDatabases();

    Optional<List<String>> getAllTables(String databaseName);

    Optional<List<String>> getAllViews(String databaseName);

    Optional<Database> getDatabase(String databaseName);

    /**
     * Adds partitions to the table in a single atomic task.  The implementation
     * must either add all partitions and return normally, or add no partitions and
     * throw an exception.
     */
    void addPartitions(String databaseName, String tableName, List<Partition> partitions);

    void alterPartition(String databaseName, String tableName, Partition partition);

    void dropPartition(String databaseName, String tableName, List<String> parts);

    void dropPartitionByName(String databaseName, String tableName, String partitionName);

    Optional<List<String>> getPartitionNames(String databaseName, String tableName);

    Optional<List<String>> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts);

    //Optional<Partition> getPartition(String databaseName, String tableName, String partitionName);
    Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues);

    Map<String, Optional<Partition>> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames);

    Optional<Table> getTable(String databaseName, String tableName);

    Set<String> getRoles(String user);

    Set<HivePrivilegeInfo> getDatabasePrivileges(String user, String databaseName);

    Set<HivePrivilegeInfo> getTablePrivileges(String user, String databaseName, String tableName);

    void grantTablePrivileges(String databaseName, String tableName, String grantee, Set<PrivilegeGrantInfo> privilegeGrantInfoSet);

    default boolean isDatabaseOwner(String user, String databaseName)
    {
        // all users are "owners" of the default database
        if (DEFAULT_DATABASE_NAME.equalsIgnoreCase(databaseName)) {
            return true;
        }

        Optional<Database> databaseMetadata = getDatabase(databaseName);
        if (!databaseMetadata.isPresent()) {
            return false;
        }

        Database database = databaseMetadata.get();

        // a database can be owned by a user or role
        if (database.getOwnerType() == USER && user.equals(database.getOwnerName())) {
            return true;
        }
        if (database.getOwnerType() == ROLE && getRoles(user).contains(database.getOwnerName())) {
            return true;
        }
        return false;
    }

    default boolean isTableOwner(String user, String databaseName, String tableName)
    {
        // a table can only be owned by a user
        Optional<Table> table = getTable(databaseName, tableName);
        return table.isPresent() && user.equals(table.get().getOwner());
    }
}
