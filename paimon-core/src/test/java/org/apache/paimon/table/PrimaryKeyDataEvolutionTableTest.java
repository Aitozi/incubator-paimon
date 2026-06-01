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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for primary-key tables with data evolution enabled. */
public class PrimaryKeyDataEvolutionTableTest extends DataEvolutionTestBase {

    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("gdpr", DataTypes.STRING())
                .primaryKey("id")
                .option(CoreOptions.BUCKET.key(), "1")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .build();
    }

    @Test
    public void testPartialColumnUpdateRead() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(
                    GenericRow.of(
                            1, BinaryString.fromString("Alice"), BinaryString.fromString("raw-a")));
            write.write(
                    GenericRow.of(
                            2, BinaryString.fromString("Bob"), BinaryString.fromString("raw-b")));
            commit.commit(write.prepareCommit());
        }

        DataFileMeta baseFile = onlyDataFile(table);
        assertThat(baseFile.firstRowId()).isNotNull();
        assertThat(baseFile.writeCols()).isNull();

        RowType partialType = schemaDefault().rowType().project(Arrays.asList("id", "gdpr"));
        builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(partialType);
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(1, BinaryString.fromString("masked-a")));
            write.write(GenericRow.of(2, BinaryString.fromString("masked-b")));
            List<CommitMessage> commitMessages = write.prepareCommit();
            setFirstRowId(commitMessages, baseFile.firstRowId());
            commit.commit(commitMessages);
        }

        List<DataFileMeta> dataFiles = dataFiles(table);
        assertThat(dataFiles).hasSize(2);
        assertThat(dataFiles)
                .filteredOn(file -> file.writeCols() != null)
                .singleElement()
                .satisfies(file -> assertThat(file.writeCols()).containsExactly("id", "gdpr"));

        assertThat(readStrings(table)).containsExactly("1,Alice,masked-a", "2,Bob,masked-b");
    }

    @Test
    public void testPartialColumnUpdateAfterCompaction() throws Exception {
        testPartialColumnUpdateRead();

        FileStoreTable table = getTableDefault();
        compact(table, BinaryRow.EMPTY_ROW, 0);

        assertThat(readStrings(table)).containsExactly("1,Alice,masked-a", "2,Bob,masked-b");
    }

    private DataFileMeta onlyDataFile(FileStoreTable table) {
        List<DataFileMeta> dataFiles = dataFiles(table);
        assertThat(dataFiles).hasSize(1);
        return dataFiles.get(0);
    }

    private List<DataFileMeta> dataFiles(FileStoreTable table) {
        TableScan.Plan plan = table.newReadBuilder().newScan().plan();
        return plan.splits().stream()
                .map(split -> (DataSplit) split)
                .flatMap(split -> split.dataFiles().stream())
                .collect(Collectors.toList());
    }

    private List<String> readStrings(FileStoreTable table) throws Exception {
        return read(table).stream().map(this::rowToString).sorted().collect(Collectors.toList());
    }

    private String rowToString(InternalRow row) {
        return row.getInt(0)
                + ","
                + row.getString(1).toString()
                + ","
                + row.getString(2).toString();
    }
}
