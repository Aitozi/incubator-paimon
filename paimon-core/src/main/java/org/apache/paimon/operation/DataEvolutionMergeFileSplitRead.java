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

package org.apache.paimon.operation;

import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.DropDeleteReader;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.mergetree.compact.ReducerMergeFunctionWrapper;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** A primary-key split read which merges data-evolution columns before merge-tree MOR. */
public class DataEvolutionMergeFileSplitRead implements SplitRead<KeyValue> {

    private final MergeFileSplitRead mergeRead;
    private final DataEvolutionSplitRead unionRead;
    private final RowType keyType;

    private RowType readValueType;
    @Nullable private RowType outerReadType;
    private boolean forceKeepDelete;

    public DataEvolutionMergeFileSplitRead(
            MergeFileSplitRead mergeRead,
            DataEvolutionSplitRead unionRead,
            RowType keyType,
            RowType valueType) {
        this.mergeRead = mergeRead;
        this.unionRead = unionRead;
        this.keyType = keyType;
        this.readValueType = valueType;
        this.unionRead.withReadType(KeyValue.schema(keyType, valueType));
    }

    public TableSchema tableSchema() {
        return mergeRead.tableSchema();
    }

    @Override
    public SplitRead<KeyValue> forceKeepDelete() {
        this.forceKeepDelete = true;
        return this;
    }

    @Override
    public SplitRead<KeyValue> withIOManager(@Nullable IOManager ioManager) {
        mergeRead.withIOManager(ioManager);
        return this;
    }

    @Override
    public SplitRead<KeyValue> withReadType(RowType readType) {
        mergeRead.withReadType(readType);
        this.readValueType = mergeRead.actualReadType();
        if (readType.getFields().equals(readValueType.getFields())) {
            this.outerReadType = null;
        } else {
            this.outerReadType = readType;
        }
        unionRead.withReadType(KeyValue.schema(keyType, readValueType));
        return this;
    }

    @Override
    public SplitRead<KeyValue> withFilter(@Nullable Predicate predicate) {
        // Data evolution union read cannot safely push filters into partial column files.
        return this;
    }

    @Override
    public RecordReader<KeyValue> createReader(Split split) throws IOException {
        if (!(split instanceof DataSplit)) {
            throw new IllegalArgumentException(
                    "DataEvolutionMergeFileSplitRead only supports DataSplit.");
        }

        DataSplit dataSplit = (DataSplit) split;
        List<ReaderSupplier<KeyValue>> readers = new ArrayList<>();
        List<List<DataFileMeta>> groups =
                DataEvolutionSplitRead.mergeRangesAndSort(dataSplit.dataFiles());
        for (List<DataFileMeta> group : groups) {
            readers.add(() -> createGroupReader(dataSplit, group));
        }

        MergeFunctionWrapper<KeyValue> mergeFunctionWrapper =
                new ReducerMergeFunctionWrapper(
                        mergeRead.mergeFunctionFactory().create(readValueType));
        RecordReader<KeyValue> reader =
                mergeRead
                        .mergeSorter()
                        .mergeSortNoSpill(
                                readers,
                                mergeRead.keyComparator(),
                                mergeRead.createUdsComparator(),
                                mergeFunctionWrapper);

        if (!forceKeepDelete) {
            reader = new DropDeleteReader(reader);
        }

        return projectOuter(reader);
    }

    private RecordReader<KeyValue> createGroupReader(DataSplit split, List<DataFileMeta> files)
            throws IOException {
        DataSplit groupSplit =
                DataSplit.builder()
                        .withSnapshot(split.snapshotId())
                        .withPartition(split.partition())
                        .withBucket(split.bucket())
                        .withBucketPath(split.bucketPath())
                        .withTotalBuckets(split.totalBuckets())
                        .withDataFiles(files)
                        .isStreaming(split.isStreaming())
                        .rawConvertible(false)
                        .build();
        KeyValueSerializer serializer = new KeyValueSerializer(keyType, readValueType);
        return unionRead.createReader(groupSplit).transform(serializer::fromRow);
    }

    private RecordReader<KeyValue> projectOuter(RecordReader<KeyValue> reader) {
        if (outerReadType == null) {
            return reader;
        }

        ProjectedRow projectedRow = ProjectedRow.from(outerReadType, readValueType);
        return reader.transform(kv -> kv.replaceValue(projectedRow.replaceRow(kv.value())));
    }
}
