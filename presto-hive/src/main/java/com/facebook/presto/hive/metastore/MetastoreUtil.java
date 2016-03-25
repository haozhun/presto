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

import com.facebook.presto.hive.HiveBucketProperty;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.ProtectMode;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde.serdeConstants;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.typeToThriftType;

public class MetastoreUtil
{
    private MetastoreUtil()
    {
    }

    public static Properties getTableMetadata(Table table)
    {
        return getSchema(
                table.getStorage(),
                table.getDataColumns(),
                table.getDataColumns(),
                table.getParameters(),
                table.getDatabaseName(),
                table.getTableName(),
                table.getPartitionColumns());
    }

    public static Properties getSchema(Partition partition, Table table)
    {
        return getSchema(
                partition.getStorage(),
                partition.getColumns(),
                table.getDataColumns(),
                table.getParameters(),
                table.getDatabaseName(),
                table.getTableName(),
                table.getPartitionColumns());
    }

    public static ProtectMode getProtectMode(Partition partition)
    {
        return getProtectMode(partition.getParameters());
    }

    public static ProtectMode getProtectMode(Table table)
    {
        return getProtectMode(table.getParameters());
    }

    public static String makePartName(List<FieldSchema> partitionColumns, List<String> values)
    {
        checkArgument(partitionColumns.size() == values.size());
        List<String> partitionColumnNames = partitionColumns.stream().map(FieldSchema::getName).collect(toList());
        return FileUtils.makePartName(partitionColumnNames, values);
    }

    public static org.apache.hadoop.hive.metastore.api.Table toMetastoreApiTable(Table table, PrincipalPrivilegeSet privileges)
    {
        org.apache.hadoop.hive.metastore.api.Table result = new org.apache.hadoop.hive.metastore.api.Table();
        result.setDbName(table.getDatabaseName());
        result.setTableName(table.getTableName());
        result.setOwner(table.getOwner());
        result.setTableType(table.getTableType());
        result.setParameters(table.getParameters());
        result.setPartitionKeys(table.getPartitionColumns().stream().map(MetastoreUtil::toMetastoreApiFieldSchema).collect(Collectors.toList()));
        result.setSd(makeStorageDescriptor(table.getTableName(), table.getDataColumns(), table.getStorage()));
        result.setPrivileges(privileges);
        result.setViewOriginalText(table.getViewOriginalText().orElse(null));
        result.setViewExpandedText(table.getViewExpandedText().orElse(null));
        return result;
    }

    public static org.apache.hadoop.hive.metastore.api.Partition toMetastoreApiPartition(Partition partition)
    {
        org.apache.hadoop.hive.metastore.api.Partition result = new org.apache.hadoop.hive.metastore.api.Partition();
        result.setDbName(partition.getDatabaseName());
        result.setTableName(partition.getTableName());
        result.setValues(partition.getValues());
        result.setSd(makeStorageDescriptor(partition.getTableName(), partition.getColumns(), partition.getStorage()));
        result.setParameters(partition.getParameters());
        return result;
    }

    public static Table fromMetastoreApiTable(org.apache.hadoop.hive.metastore.api.Table table)
    {
        StorageDescriptor storageDescriptor = table.getSd();
        if (storageDescriptor == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Table is missing storage descriptor");
        }

        Table.Builder tableBuilder = new Table.Builder()
                .setDatabaseName(table.getDbName())
                .setTableName(table.getTableName())
                .setOwner(table.getOwner())
                .setTableType(table.getTableType())
                .setDataColumns(storageDescriptor.getCols().stream()
                        .map(MetastoreUtil::fromMetastoreApiFieldSchema)
                        .collect(Collectors.toList()))
                .setPartitionColumns(table.getPartitionKeys().stream()
                        .map(MetastoreUtil::fromMetastoreApiFieldSchema)
                        .collect(Collectors.toList()))
                .setParameters(table.getParameters() == null ? ImmutableMap.of() : table.getParameters())
                .setViewOriginalText(makeOptional(table.getViewOriginalText()))
                .setViewExpandedText(makeOptional(table.getViewExpandedText()));

        fromMetastoreApiStorageDescriptor(storageDescriptor, tableBuilder.getStorageBuilder(), table.getTableName());

        return tableBuilder.build();
    }

    public static Partition fromMetastoreApiPartition(org.apache.hadoop.hive.metastore.api.Partition partition)
    {
        StorageDescriptor storageDescriptor = partition.getSd();
        if (storageDescriptor == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Partition is missing storage descriptor");
        }

        Partition.Builder partitionBuilder = new Partition.Builder()
                .setDatabaseName(partition.getDbName())
                .setTableName(partition.getTableName())
                .setValues(partition.getValues())
                .setColumns(storageDescriptor.getCols().stream()
                        .map(MetastoreUtil::fromMetastoreApiFieldSchema)
                        .collect(Collectors.toList()))
                .setParameters(partition.getParameters());

        fromMetastoreApiStorageDescriptor(storageDescriptor, partitionBuilder.getStorageBuilder(), format("%s.%s", partition.getTableName(), partition.getValues()));

        return partitionBuilder.build();
    }

    private static Properties getSchema(
            Storage sd,
            List<FieldSchema> dataColumns,
            List<FieldSchema> tableDataColumns,
            Map<String, String> parameters,
            String databaseName,
            String tableName,
            List<FieldSchema> partitionKeys)
    {
        // Adapted from MetaStoreUtils.getSchema in Hive
        Properties schema = new Properties();

        schema.setProperty(hive_metastoreConstants.FILE_INPUT_FORMAT, sd.getStorageFormat().getInputFormat());
        schema.setProperty(hive_metastoreConstants.FILE_OUTPUT_FORMAT, sd.getStorageFormat().getOutputFormat());

        schema.setProperty(hive_metastoreConstants.META_TABLE_NAME, databaseName + "." + tableName);
        schema.setProperty(hive_metastoreConstants.META_TABLE_LOCATION, sd.getLocation());

        if (sd.getBucketProperty().isPresent()) {
            schema.setProperty(hive_metastoreConstants.BUCKET_FIELD_NAME, sd.getBucketProperty().get().getClusteredBy().get(0));
            schema.setProperty(hive_metastoreConstants.BUCKET_COUNT, Integer.toString(sd.getBucketProperty().get().getBucketCount()));
        }
        else {
            schema.setProperty(hive_metastoreConstants.BUCKET_COUNT, "0");
        }

        for (Map.Entry<String, String> param : sd.getSerdeParameters().entrySet()) {
            schema.setProperty(param.getKey(), (param.getValue() != null) ? param.getValue() : "");
        }
        schema.setProperty(serdeConstants.SERIALIZATION_LIB, sd.getStorageFormat().getSerDe());

        StringBuilder colNameBuf = new StringBuilder();
        StringBuilder colTypeBuf = new StringBuilder();
        StringBuilder colComment = new StringBuilder();
        boolean first = true;
        for (FieldSchema col : tableDataColumns) {
            if (!first) {
                colNameBuf.append(",");
                colTypeBuf.append(":");
                colComment.append('\0');
            }
            colNameBuf.append(col.getName());
            colTypeBuf.append(col.getType());
            colComment.append((null != col.getComment()) ? col.getComment() : "");
            first = false;
        }
        String colNames = colNameBuf.toString();
        String colTypes = colTypeBuf.toString();
        schema.setProperty(hive_metastoreConstants.META_TABLE_COLUMNS, colNames);
        schema.setProperty(hive_metastoreConstants.META_TABLE_COLUMN_TYPES, colTypes);
        schema.setProperty("columns.comments", colComment.toString());

        schema.setProperty(serdeConstants.SERIALIZATION_DDL, getDDLFromFieldSchema(tableName, dataColumns));

        String partString = "";
        String partStringSep = "";
        String partTypesString = "";
        String partTypesStringSep = "";
        for (FieldSchema partKey : partitionKeys) {
            partString = partString.concat(partStringSep);
            partString = partString.concat(partKey.getName());
            partTypesString = partTypesString.concat(partTypesStringSep);
            partTypesString = partTypesString.concat(partKey.getType().getHiveTypeName());
            if (partStringSep.length() == 0) {
                partStringSep = "/";
                partTypesStringSep = ":";
            }
        }
        if (partString.length() > 0) {
            schema.setProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, partString);
            schema.setProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES, partTypesString);
        }

        if (parameters != null) {
            for (Map.Entry<String, String> e : parameters.entrySet()) {
                // add non-null parameters to the schema
                if (e.getValue() != null) {
                    schema.setProperty(e.getKey(), e.getValue());
                }
            }
        }

        return schema;
    }

    private static String getDDLFromFieldSchema(String structName, List<FieldSchema> fieldSchemas)
    {
        StringBuilder ddl = new StringBuilder();
        ddl.append("struct ");
        ddl.append(structName);
        ddl.append(" { ");
        boolean first = true;
        for (FieldSchema col : fieldSchemas) {
            if (first) {
                first = false;
            }
            else {
                ddl.append(", ");
            }
            ddl.append(typeToThriftType(col.getType().getHiveTypeName()));
            ddl.append(' ');
            ddl.append(col.getName());
        }
        ddl.append("}");
        return ddl.toString();
    }

    private static ProtectMode getProtectMode(Map<String, String> parameters)
    {
        if (parameters == null) {
            return null;
        }

        if (!parameters.containsKey(ProtectMode.PARAMETER_NAME)) {
            return new ProtectMode();
        }
        else {
            return ProtectMode.getProtectModeFromString(parameters.get(ProtectMode.PARAMETER_NAME));
        }
    }

    private static StorageDescriptor makeStorageDescriptor(String tableName, List<FieldSchema> columns, Storage storage)
    {
        if (storage.isSorted() || storage.isSkewed()) {
            throw new IllegalArgumentException("Writing to sorted and/or skewed table/partition is not supported");
        }
        SerDeInfo serdeInfo = new SerDeInfo();
        serdeInfo.setName(tableName);
        serdeInfo.setSerializationLib(storage.getStorageFormat().getSerDeNullable());
        serdeInfo.setParameters(ImmutableMap.of());

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(emptyToNull(storage.getLocation()));
        sd.setCols(columns.stream()
                .map(MetastoreUtil::toMetastoreApiFieldSchema)
                .collect(Collectors.toList()));
        sd.setSerdeInfo(serdeInfo);
        sd.setInputFormat(storage.getStorageFormat().getInputFormatNullable());
        sd.setOutputFormat(storage.getStorageFormat().getOutputFormatNullable());
        sd.setParameters(ImmutableMap.of());
        return sd;
    }

    private static org.apache.hadoop.hive.metastore.api.FieldSchema toMetastoreApiFieldSchema(FieldSchema fieldSchema)
    {
        return new org.apache.hadoop.hive.metastore.api.FieldSchema(fieldSchema.getName(), fieldSchema.getType().getHiveTypeName(), fieldSchema.getComment().orElse(null));
    }

    private static FieldSchema fromMetastoreApiFieldSchema(org.apache.hadoop.hive.metastore.api.FieldSchema fieldSchema)
    {
        return new FieldSchema(fieldSchema.getName(), HiveType.valueOf(fieldSchema.getType()), makeOptional(fieldSchema.getComment()));
    }

    private static void fromMetastoreApiStorageDescriptor(StorageDescriptor storageDescriptor, Storage.Builder builder, String tablePartitionName)
    {
        SerDeInfo serdeInfo = storageDescriptor.getSerdeInfo();
        if (serdeInfo == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Table storage descriptor is missing SerDe info");
        }

        builder.setStorageFormat(StorageFormat.createNullable(serdeInfo.getSerializationLib(), storageDescriptor.getInputFormat(), storageDescriptor.getOutputFormat()))
                .setLocation(nullToEmpty(storageDescriptor.getLocation()))
                .setBucketProperty(HiveBucketProperty.fromStorageDescriptor(storageDescriptor, tablePartitionName))
                .setSorted(storageDescriptor.isSetSortCols() && !storageDescriptor.getSortCols().isEmpty())
                .setSkewed(storageDescriptor.isSetSkewedInfo() && storageDescriptor.getSkewedInfo().isSetSkewedColNames() && !storageDescriptor.getSkewedInfo().getSkewedColNames().isEmpty())
                .setSerdeParameters(serdeInfo.getParameters() == null ? ImmutableMap.of() : serdeInfo.getParameters());
    }

    private static Optional<String> makeOptional(String string)
    {
        if (isNullOrEmpty(string)) {
            return Optional.empty();
        }
        return Optional.of(string);
    }
}
