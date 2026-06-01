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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A file reader factory which reads one primary-key data-evolution alignment group once. */
public class DataEvolutionKeyValueFileReaderFactory implements FileReaderFactory<KeyValue> {

    private final BinaryRow partition;
    private final int bucket;
    private final DataEvolutionSplitRead unionRead;
    private final RowType keyType;
    private final RowType valueType;
    private final Map<String, List<DataFileMeta>> fileToGroup;
    private final Set<String> groupLeaders;

    public DataEvolutionKeyValueFileReaderFactory(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            DataEvolutionSplitRead unionRead,
            RowType keyType,
            RowType valueType) {
        this.partition = partition;
        this.bucket = bucket;
        this.unionRead = unionRead;
        this.keyType = keyType;
        this.valueType = valueType;
        this.fileToGroup = new HashMap<>();
        this.groupLeaders = new HashSet<>();

        for (List<DataFileMeta> group : DataEvolutionSplitRead.mergeRangesAndSort(files)) {
            groupLeaders.add(group.get(0).fileName());
            for (DataFileMeta file : group) {
                fileToGroup.put(file.fileName(), group);
            }
        }
    }

    @Override
    public RecordReader<KeyValue> createRecordReader(DataFileMeta file) throws IOException {
        if (!groupLeaders.contains(file.fileName())) {
            return emptyReader();
        }

        DataSplit groupSplit =
                DataSplit.builder()
                        .withSnapshot(0)
                        .withPartition(partition)
                        .withBucket(bucket)
                        .withBucketPath("")
                        .withDataFiles(fileToGroup.get(file.fileName()))
                        .rawConvertible(false)
                        .build();
        KeyValueSerializer serializer = new KeyValueSerializer(keyType, valueType);
        return unionRead.createReader(groupSplit).transform(serializer::fromRow);
    }

    private RecordReader<KeyValue> emptyReader() {
        return new RecordReader<KeyValue>() {
            @Override
            public RecordIterator<KeyValue> readBatch() {
                return null;
            }

            @Override
            public void close() {}
        };
    }
}
