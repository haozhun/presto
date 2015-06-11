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
package com.facebook.presto.orc;

import com.facebook.presto.hive.$internal.com.google.common.collect.ImmutableList;
import com.facebook.presto.orc.metadata.DwrfMetadataReader;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@SuppressWarnings("UseOfSystemOutOrSystemErr")
public final class ScanDwrfFile
{
    private ScanDwrfFile()
    {
    }

    public static void main(String[] args)
            throws Exception
    {
        File file = new File("/Users/hjin/orc-test/orctest.orc");
        //OrcReader orcReader = new OrcReader(new FileOrcDataSource(file, new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE)), new DwrfMetadataReader());
        OrcReader orcReader = new OrcReader(new FileOrcDataSource(file, new DataSize(1, Unit.MEGABYTE), new DataSize(Integer.MAX_VALUE, Unit.BYTE), new DataSize(0, Unit.BYTE)), new DwrfMetadataReader());
        List<OrcType> columnTypes = getColumnTypes(orcReader);
        Map<Integer, Type> columns = new TreeMap<>();
        for (int i = 0; i < columnTypes.size(); i++) {
            // This type is arbitrarily chosen because the value is not significant unless it's a structural type
            columns.put(i, VarcharType.VARCHAR);
        }
        OrcRecordReader recordReader = orcReader.createRecordReader(columns, OrcPredicate.TRUE, DateTimeZone.getDefault());

        int batchCount = 0;
        int rows = 0;

        /*
        System.out.print("Read None:\n\t");
        for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
            readBatch(columnTypes, recordReader, batchSize);
            rows += batchSize;
            batchCount++;
            if (batchCount % 10 == 0) {
                System.out.println();
            }
        }
        System.out.println();
        System.out.println("rows: " + rows);
        */

        System.out.print("Read All:\n\t");
        rows = 0;
        for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
            readBatch(columnTypes, recordReader, batchSize);
            rows += batchSize;
        }
        System.out.println();
        System.out.println("rows: " + rows);

        if (true) {
            return;
        }

        System.out.println();
        System.out.print("Read One Batch:\n\t");
        rows = 0;
        for (int batchToRead = 0; batchToRead < batchCount; batchToRead++) {
            recordReader = orcReader.createRecordReader(columns, OrcPredicate.TRUE, DateTimeZone.getDefault());
            int batchId = 0;
            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                if (batchId == batchToRead) {
                    readBatch(columnTypes, recordReader, batchSize);
                    rows += batchSize;
                    break;
                }
                batchId++;
                if (batchId % 100 == 0) {
                    System.out.println();
                }
            }
        }
        System.out.println();
        System.out.println("rows: " + rows);

        System.out.println();
        System.out.print("Read Two Batches:\n\t");
        rows = 0;
        for (int batchToRead = 0; batchToRead < batchCount; batchToRead++) {
            recordReader = orcReader.createRecordReader(columns, OrcPredicate.TRUE, DateTimeZone.getDefault());
            int batchId = 0;
            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                if (batchId == batchToRead) {
                    readBatch(columnTypes, recordReader, batchSize);
                    rows += batchSize;
                }
                if (batchId == batchToRead + 1) {
                    readBatch(columnTypes, recordReader, batchSize);
                    break;
                }
                batchId++;
            }
        }

        System.out.println();
        System.out.println("rows: " + rows);

        System.out.println();
        System.out.print("Read One, Skip One, Read One:\n\t");
        rows = 0;
        for (int batchToRead = 0; batchToRead < batchCount; batchToRead++) {
            recordReader = orcReader.createRecordReader(columns, OrcPredicate.TRUE, DateTimeZone.getDefault());
            int batchId = 0;
            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                if (batchId == batchToRead) {
                    readBatch(columnTypes, recordReader, batchSize);
                    rows += batchSize;
                }
                if (batchId == batchToRead + 2) {
                    readBatch(columnTypes, recordReader, batchSize);
                    break;
                }
                batchId++;
            }
        }

        System.out.println();
        System.out.println("rows: " + rows);
        System.out.println();
        System.out.println("done");
    }

    private static void readBatch(List<OrcType> columnTypes, OrcRecordReader recordReader, int batchSize)
            throws IOException
    {
        for (int fieldId = 0; fieldId < columnTypes.size(); fieldId++) {
            Vector vector = readColumnBatch(columnTypes, recordReader, batchSize, fieldId);
//            String objectVector = vector.toObjectVector(batchSize).toString();
//            System.out.print(objectVector.charAt(objectVector.length() - 1));
        }
    }

    private static Vector readColumnBatch(List<OrcType> columnTypes, OrcRecordReader recordReader, int batchSize, int fieldId)
            throws IOException
    {
        Vector vector;

        OrcType columnType = columnTypes.get(fieldId);
        switch (columnType.getOrcTypeKind()) {
            case BOOLEAN:
                vector = new BooleanVector(batchSize);
                break;
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case DECIMAL:
            case DATE:
            case TIMESTAMP:
                vector = new LongVector(batchSize);
                break;
            case FLOAT:
            case DOUBLE:
                vector = new DoubleVector(batchSize);
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
            case BINARY:
            case LIST:
            case MAP:
            case STRUCT:
            case UNION:
                vector = new SliceVector(batchSize);
                break;
            default:
                throw new IllegalArgumentException("Unknown type " + columnType);
        }
        recordReader.readVector(fieldId, vector);
        return vector;
    }

    public static List<OrcType> getColumnTypes(OrcReader orcReader)
    {
        List<OrcType> types = orcReader.getFooter().getTypes();
        OrcType rowType = types.get(0);
        ImmutableList.Builder<OrcType> columnTypes = ImmutableList.builder();
        for (int fieldId = 0; fieldId < rowType.getFieldCount(); fieldId++) {
            OrcType fieldType = types.get(rowType.getFieldTypeIndex(fieldId));
            columnTypes.add(fieldType);
        }
        return columnTypes.build();
    }
}
