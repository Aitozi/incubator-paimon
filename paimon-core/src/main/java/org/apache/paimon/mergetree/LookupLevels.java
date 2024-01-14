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

package org.apache.paimon.mergetree;

import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.lookup.LookupStoreWriter;
import org.apache.paimon.lookup.bloom.PaimonBloomFilter;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.IOFunction;
import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.function.Supplier;

import static org.apache.paimon.mergetree.LookupUtils.fileKibiBytes;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Provide lookup by key. */
public class LookupLevels implements Levels.DropFileCallback, Closeable {

    private final Levels levels;
    private final Comparator<InternalRow> keyComparator;
    private final RowCompactedSerializer keySerializer;
    private final RowCompactedSerializer valueSerializer;
    private final IOFunction<DataFileMeta, RecordReader<KeyValue>> fileReaderFactory;
    private final Supplier<File> localFileFactory;
    private final LookupStoreFactory lookupStoreFactory;

    private final Cache<String, LookupFile> lookupFiles;
    private final Cache<String, IndexFile> indexFiles;

    public LookupLevels(
            Levels levels,
            Comparator<InternalRow> keyComparator,
            RowType keyType,
            RowType valueType,
            IOFunction<DataFileMeta, RecordReader<KeyValue>> fileReaderFactory,
            Supplier<File> localFileFactory,
            LookupStoreFactory lookupStoreFactory,
            Duration fileRetention,
            MemorySize maxDiskSize) {
        this.levels = levels;
        this.keyComparator = keyComparator;
        this.keySerializer = new RowCompactedSerializer(keyType);
        this.valueSerializer = new RowCompactedSerializer(valueType);
        this.fileReaderFactory = fileReaderFactory;
        this.localFileFactory = localFileFactory;
        this.lookupStoreFactory = lookupStoreFactory;
        this.lookupFiles =
                Caffeine.newBuilder()
                        .expireAfterAccess(fileRetention)
                        .maximumWeight(maxDiskSize.getKibiBytes())
                        .weigher(this::fileWeigh)
                        .removalListener(this::removalCallback)
                        .executor(MoreExecutors.directExecutor())
                        .build();
        // bloom filter 过期配置单独调整 例如64M缓存, 1d的过期时间 ?
        this.indexFiles =
                Caffeine.newBuilder()
                        .expireAfterAccess(fileRetention)
                        .maximumWeight(maxDiskSize.getKibiBytes())
                        .weigher(this::fileWeigh)
                        .removalListener(this::removalCallback)
                        .executor(MoreExecutors.directExecutor())
                        .build();

        levels.addDropFileCallback(this);
    }

    public Levels getLevels() {
        return levels;
    }

    @VisibleForTesting
    Cache<String, LookupFile> lookupFiles() {
        return lookupFiles;
    }

    @Override
    public void notifyDropFile(String file) {
        lookupFiles.invalidate(file);
        indexFiles.invalidate(file);
    }

    @Nullable
    public KeyValue lookup(InternalRow key, int startLevel) throws IOException {
        return LookupUtils.lookup(levels, key, startLevel, this::lookup, this::lookupLevel0);
    }

    @Nullable
    private KeyValue lookupLevel0(InternalRow key, TreeSet<DataFileMeta> level0)
            throws IOException {
        return LookupUtils.lookupLevel0(keyComparator, key, level0, this::lookup);
    }

    @Nullable
    private KeyValue lookup(InternalRow key, SortedRun level) throws IOException {
        return LookupUtils.lookup(keyComparator, key, level, this::lookup);
    }

    @Nullable
    private KeyValue lookup(InternalRow key, DataFileMeta file) throws IOException {
        IndexFile indexFile = indexFiles.getIfPresent(file.fileName());
        LookupFile lookupFile = lookupFiles.getIfPresent(file.fileName());

        byte[] keyBytes = keySerializer.serializeToBytes(key);
        if (indexFile != null && !indexFile.mightContains(keyBytes)) {
            return null;
        }

        while (lookupFile == null || lookupFile.isClosed) {
            // 是不是可以避免创建一次 bloomfilter. 复用前面的
            Pair<LookupFile, IndexFile> local = createLookupFile(file);
            lookupFiles.put(file.fileName(), local.getLeft());
            // 直接更新旧值, 会触发callback吗?
            indexFiles.put(file.fileName(), local.getRight());
            indexFile = local.getRight();
            lookupFile = local.getLeft();
        }

        if (!indexFile.mightContains(keyBytes)) {
            return null;
        }

        byte[] valueBytes = lookupFile.get(keyBytes);
        if (valueBytes == null) {
            return null;
        }
        InternalRow value = valueSerializer.deserialize(valueBytes);
        long sequenceNumber = MemorySegment.wrap(valueBytes).getLong(valueBytes.length - 9);
        RowKind rowKind = RowKind.fromByteValue(valueBytes[valueBytes.length - 1]);
        return new KeyValue()
                .replace(key, sequenceNumber, rowKind, value)
                .setLevel(lookupFile.remoteFile().level());
    }

    private int fileWeigh(String file, CacheFile lookupFile) {
        return fileKibiBytes(lookupFile.localFile);
    }

    private void removalCallback(String key, CacheFile file, RemovalCause cause) {
        if (file != null) {
            try {
                file.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private Pair<LookupFile, IndexFile> createLookupFile(DataFileMeta file) throws IOException {
        File localFile = localFileFactory.get();
        File indexFile = new File(localFile.getAbsolutePath() + ".bf");
        if (!localFile.createNewFile()) {
            throw new IOException("Can not create new file: " + localFile);
        }
        if (!indexFile.createNewFile()) {
            throw new IOException("Can not create new file: " + localFile);
        }
        PaimonBloomFilter bloomFilter = new PaimonBloomFilter(indexFile, file.rowCount());
        try (LookupStoreWriter kvWriter = lookupStoreFactory.createWriter(localFile);
                RecordReader<KeyValue> reader = fileReaderFactory.apply(file)) {
            DataOutputSerializer valueOut = new DataOutputSerializer(32);
            RecordReader.RecordIterator<KeyValue> batch;
            KeyValue kv;
            while ((batch = reader.readBatch()) != null) {
                while ((kv = batch.next()) != null) {
                    byte[] keyBytes = keySerializer.serializeToBytes(kv.key());
                    valueOut.clear();
                    valueOut.write(valueSerializer.serializeToBytes(kv.value()));
                    valueOut.writeLong(kv.sequenceNumber());
                    valueOut.writeByte(kv.valueKind().toByteValue());
                    byte[] valueBytes = valueOut.getCopyOfBuffer();
                    kvWriter.put(keyBytes, valueBytes);
                    bloomFilter.add(keyBytes);
                }
                batch.releaseBatch();
            }
        } catch (IOException e) {
            FileIOUtils.deleteFileOrDirectory(localFile);
            FileIOUtils.deleteFileOrDirectory(indexFile);
            throw e;
        }
        bloomFilter.finish();

        return Pair.of(
                new LookupFile(localFile, file, lookupStoreFactory.createReader(localFile)),
                new IndexFile(indexFile, file));
    }

    @Override
    public void close() throws IOException {
        lookupFiles.invalidateAll();
    }

    public static class CacheFile implements Closeable {
        private final File localFile;
        private final DataFileMeta remoteFile;

        protected boolean isClosed = false;

        public CacheFile(File localFile, DataFileMeta remoteFile) {
            this.localFile = localFile;
            this.remoteFile = remoteFile;
        }

        public DataFileMeta remoteFile() {
            return remoteFile;
        }

        @Override
        public void close() throws IOException {
            isClosed = true;
            FileIOUtils.deleteFileOrDirectory(localFile);
        }
    }

    private static class IndexFile extends CacheFile {

        private PaimonBloomFilter.BloomFilterReader reader;

        public IndexFile(File localFile, DataFileMeta remoteFile) throws IOException {
            super(localFile, remoteFile);
            this.reader = PaimonBloomFilter.createReader(localFile);
        }

        @Nullable
        public boolean mightContains(byte[] key) throws IOException {
            checkArgument(!isClosed);
            return reader.mightContains(key);
        }
    }

    private static class LookupFile extends CacheFile {

        private final LookupStoreReader reader;

        public LookupFile(File localFile, DataFileMeta remoteFile, LookupStoreReader reader) {
            super(localFile, remoteFile);
            this.reader = reader;
        }

        @Nullable
        public byte[] get(byte[] key) throws IOException {
            checkArgument(!isClosed);
            return reader.lookup(key);
        }

        @Override
        public void close() throws IOException {
            reader.close();
            super.close();
        }
    }
}
