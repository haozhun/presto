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

import com.facebook.presto.orc.memory.AggregatedMemoryContext;
import com.facebook.presto.orc.metadata.OrcMetadataReader;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

@SuppressWarnings("UseOfSystemOutOrSystemErr")
public final class ScanOrcFile
{
    private ScanOrcFile()
    {
    }

    public static void main(String... args)
            throws Exception
    {
        File file = new File(args[0]);
        FileOrcDataSource orcDataSource = new FileOrcDataSource(file, new DataSize(1, MEGABYTE), new DataSize(8, MEGABYTE), new DataSize(8, MEGABYTE));
        OrcReader orcReader = new OrcReader(orcDataSource, new OrcMetadataReader(), new DataSize(1, MEGABYTE), new DataSize(8, MEGABYTE));

        //System.out.println(orcReader.getColumnNames());

        /*
        "time" >= "$literal$timestamp"(1464850800000)
        "time" < "$literal$timestamp"(1465542000000)
        "userid" <> BIGINT '0'
        (NOT ("userid" IS NULL)))
        "commerce_type" IN (CAST('2' AS VARCHAR))
        "target_type" IN (CAST('PRODUCT_ITEM' AS VARCHAR))
         */
        TupleDomainOrcPredicate<String> orcPredicate = new TupleDomainOrcPredicate<>(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "time", Domain.create(ValueSet.ofRanges(Range.range(TIMESTAMP, 1464850800000L, true, 1465542000000L, false)), false),
                        "userid", Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 0L), Range.greaterThan(BIGINT, 0L)), false),
                        "commerce_type", Domain.singleValue(VARCHAR, Slices.utf8Slice("2")),
                        "target_type", Domain.singleValue(VARCHAR, Slices.utf8Slice("PRODUCT_ITEM")))),
                ImmutableList.of(
                        new TupleDomainOrcPredicate.ColumnReference<>("userid", 0, BIGINT),
                        new TupleDomainOrcPredicate.ColumnReference<>("time", 1, TIMESTAMP),
                        new TupleDomainOrcPredicate.ColumnReference<>("event", 2, VARCHAR),
                        new TupleDomainOrcPredicate.ColumnReference<>("target_type", 6, VARCHAR),
                        new TupleDomainOrcPredicate.ColumnReference<>("commerce_type", 9, VARCHAR)));
        //
        // Set your column types here
        //
        Map<Integer, Type> columnTypes = ImmutableMap.<Integer, Type>builder()
                .put(0, BIGINT) // userid
                .put(1, TIMESTAMP) // time
                .put(2, VARCHAR) // event
                .put(6, VARCHAR) // target_type
                .put(9, VARCHAR) // commerce_type
                .build();
        OrcRecordReader recordReader = orcReader.createRecordReader(columnTypes, orcPredicate, DateTimeZone.getDefault(), new AggregatedMemoryContext());

        long rows = 0;
        for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
            rows += readBatch(columnTypes, recordReader);
        }
        System.out.println();
        System.out.println("rows: " + rows);
    }

    private static int readBatch(Map<Integer, Type> columnTypes, OrcRecordReader recordReader)
            throws IOException
    {
        int batchSize = recordReader.nextBatch();
        for (Entry<Integer, Type> entry : columnTypes.entrySet()) {
            Block block = recordReader.readBlock(entry.getValue(), entry.getKey());
            block.assureLoaded();
            if (false) {
                System.out.print(entry.getValue().getObjectValue(TestingConnectorSession.SESSION, block, 0));
                System.out.print(",");
            }
        }
        //System.out.println();
        return batchSize;
    }
}
