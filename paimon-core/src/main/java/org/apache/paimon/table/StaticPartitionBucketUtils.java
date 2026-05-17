/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.BUCKET;

/** Utilities for preserving bucket number when writing a complete static partition. */
public class StaticPartitionBucketUtils {

    public static FileStoreTable tableWithHistoricalBucket(
            FileStoreTable table, @Nullable Map<String, String> staticPartition) {
        Integer totalBuckets = historicalBucket(table, staticPartition);
        if (totalBuckets == null) {
            return table;
        }

        return table.copy(Collections.singletonMap(BUCKET.key(), String.valueOf(totalBuckets)));
    }

    @Nullable
    public static Integer historicalBucket(
            FileStoreTable table, @Nullable Map<String, String> staticPartition) {
        if (!shouldResolveHistoricalBucket(table, staticPartition)) {
            return null;
        }

        BinaryRow partition = toBinaryPartition(table, staticPartition);
        Integer totalBuckets = null;
        List<SimpleFileEntry> entries =
                table.store()
                        .newScan()
                        .withPartitionFilter(Collections.singletonList(partition))
                        .onlyReadRealBuckets()
                        .readSimpleEntries();
        for (SimpleFileEntry entry : entries) {
            if (entry.totalBuckets() < 0) {
                continue;
            }

            if (totalBuckets == null) {
                totalBuckets = entry.totalBuckets();
            } else if (totalBuckets != entry.totalBuckets()) {
                throw new IllegalStateException(
                        "Partition "
                                + partition
                                + " has different totalBuckets "
                                + totalBuckets
                                + " and "
                                + entry.totalBuckets());
            }
        }
        return totalBuckets;
    }

    private static boolean shouldResolveHistoricalBucket(
            FileStoreTable table, @Nullable Map<String, String> staticPartition) {
        if (staticPartition == null || staticPartition.isEmpty()) {
            return false;
        }

        CoreOptions options = table.coreOptions();
        if (table.bucketMode() != BucketMode.HASH_FIXED || options.writeOnly()) {
            return false;
        }

        List<String> partitionKeys = table.partitionKeys();
        return staticPartition.size() == partitionKeys.size()
                && staticPartition.keySet().containsAll(partitionKeys);
    }

    private static BinaryRow toBinaryPartition(
            FileStoreTable table, Map<String, String> staticPartition) {
        RowType partitionType = table.schema().logicalPartitionType();
        GenericRow row =
                InternalRowPartitionComputer.convertSpecToInternalRow(
                        staticPartition, partitionType, table.coreOptions().partitionDefaultName());
        return new InternalRowSerializer(partitionType).toBinaryRow(row).copy();
    }

    private StaticPartitionBucketUtils() {}
}
