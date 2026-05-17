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

package org.apache.paimon.table.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.StaticPartitionBucketUtils;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.createCommitUser;

/** Implementation for {@link WriteBuilder}. */
public class BatchWriteBuilderImpl implements BatchWriteBuilder {

    private static final long serialVersionUID = 1L;

    private final InnerTable table;
    private final String commitUser;

    private InnerTable runtimeTable;
    private Map<String, String> staticPartition;
    private boolean appendCommitCheckConflict = false;
    private @Nullable Long rowIdCheckFromSnapshot = null;

    public BatchWriteBuilderImpl(InnerTable table) {
        this.table = table;
        this.commitUser = createCommitUser(new Options(table.options()));
        this.runtimeTable = table;
    }

    private BatchWriteBuilderImpl(
            InnerTable table,
            String commitUser,
            @Nullable Map<String, String> staticPartition,
            boolean appendCommitCheckConflict,
            @Nullable Long rowIdCheckFromSnapshot) {
        this.table = table;
        this.commitUser = commitUser;
        this.staticPartition = staticPartition;
        this.appendCommitCheckConflict = appendCommitCheckConflict;
        this.rowIdCheckFromSnapshot = rowIdCheckFromSnapshot;
        this.runtimeTable = runtimeTable(table, staticPartition);
    }

    @Override
    public String tableName() {
        return table.name();
    }

    @Override
    public RowType rowType() {
        return table.rowType();
    }

    @Override
    public Optional<WriteSelector> newWriteSelector() {
        return runtimeTable.newWriteSelector();
    }

    @Override
    public BatchWriteBuilder withOverwrite(@Nullable Map<String, String> staticPartition) {
        this.staticPartition = staticPartition;
        this.runtimeTable = runtimeTable(table, staticPartition);
        return this;
    }

    @Override
    public BatchTableWrite newWrite() {
        return runtimeTable.newWrite(commitUser).withIgnorePreviousFiles(staticPartition != null);
    }

    @Override
    public BatchTableCommit newCommit() {
        InnerTableCommit commit =
                runtimeTable
                        .newCommit(commitUser)
                        .withOverwrite(staticPartition)
                        .appendCommitCheckConflict(appendCommitCheckConflict)
                        .rowIdCheckConflict(rowIdCheckFromSnapshot);
        commit.ignoreEmptyCommit(snapshotIgnoreEmptyCommit(runtimeTable));
        return commit;
    }

    public BatchWriteBuilderImpl copyWithNewTable(Table newTable) {
        return new BatchWriteBuilderImpl(
                (InnerTable) newTable,
                commitUser,
                staticPartition,
                appendCommitCheckConflict,
                rowIdCheckFromSnapshot);
    }

    public BatchWriteBuilderImpl appendCommitCheckConflict(boolean appendCommitCheckConflict) {
        this.appendCommitCheckConflict = appendCommitCheckConflict;
        return this;
    }

    public BatchWriteBuilderImpl rowIdCheckConflict(@Nullable Long rowIdCheckFromSnapshot) {
        this.rowIdCheckFromSnapshot = rowIdCheckFromSnapshot;
        return this;
    }

    private static InnerTable runtimeTable(
            InnerTable table, @Nullable Map<String, String> staticPartition) {
        if (table instanceof FileStoreTable) {
            return StaticPartitionBucketUtils.tableWithHistoricalBucket(
                    (FileStoreTable) table, staticPartition);
        }
        return table;
    }

    private static boolean snapshotIgnoreEmptyCommit(InnerTable table) {
        return Options.fromMap(table.options())
                .getOptional(CoreOptions.SNAPSHOT_IGNORE_EMPTY_COMMIT)
                .orElse(true);
    }
}
