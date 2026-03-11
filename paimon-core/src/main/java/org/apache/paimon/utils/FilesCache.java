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

package org.apache.paimon.utils;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.MemorySize;

import org.apache.paimon.shade.guava30.com.google.common.cache.Cache;
import org.apache.paimon.shade.guava30.com.google.common.cache.CacheBuilder;
import org.apache.paimon.shade.guava30.com.google.common.cache.RemovalNotification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Cache based on local files. */
public class FilesCache implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(FilesCache.class);

    private static final String PREFIX = "paimon";

    private final Cache<Path, CachedFile> cache;
    private final FileIO cacheFileIO;
    private final Path cachePath;

    public FilesCache(MemorySize diskSize, Path cacheDir, FileIO cacheFileIO) {
        this(diskSize, cacheDir, cacheFileIO, Duration.ZERO);
    }

    public FilesCache(
            MemorySize diskSize, Path cacheDir, FileIO cacheFileIO, Duration expireTime) {
        CacheBuilder<Path, CachedFile> builder =
                CacheBuilder.newBuilder()
                        .weigher(this::weigh)
                        .maximumWeight(diskSize.getKibiBytes())
                        .recordStats()
                        .removalListener(this::removeCallback);
        if (!expireTime.isZero() && !expireTime.isNegative()) {
            builder = builder.expireAfterAccess(expireTime);
        }

        this.cache = builder.build();
        this.cacheFileIO = cacheFileIO;
        this.cachePath = cacheDir;
        LOG.info("Init file cache in {}.", cachePath);
    }

    public FileIO getCacheFileIO() {
        return cacheFileIO;
    }

    public Cache<Path, CachedFile> getCache() {
        return cache;
    }

    public CachedFile load(Path path, FileIO fileIO) throws IOException {
        long start = System.currentTimeMillis();
        Path localPath =
                new Path(cachePath, PREFIX + "-" + path.getName() + "-" + UUID.randomUUID());
        try (SeekableInputStream input = fileIO.newInputStream(path);
                PositionOutputStream output = cacheFileIO.newOutputStream(localPath, false)) {
            IOUtils.copyBytes(input, output);
            long size = cacheFileIO.getFileSize(localPath);
            CachedFile cachedFile =
                    new CachedFile(
                            size,
                            localPath,
                            () -> {
                                cacheFileIO.deleteQuietly(localPath);
                                LOG.info("Delete local cached file {}.", localPath);
                            });
            LOG.info(
                    "Use {} ms to load file {} to local cache {}.",
                    System.currentTimeMillis() - start,
                    path,
                    localPath);
            return cachedFile;
        } catch (Exception e) {
            cacheFileIO.deleteQuietly(localPath);
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new RuntimeException(e);
        }
    }

    public CachedFile get(Path key, FileIO fileIO) throws IOException {
        try {
            return cache.get(key, () -> load(key, fileIO)).incRef();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    private void removeCallback(RemovalNotification<Path, CachedFile> notification) {
        CachedFile cachedFile = notification.getValue();
        if (cachedFile == null) {
            return;
        }

        LOG.info(
                "Local cached file {} removed from cache due to {}.",
                cachedFile.path,
                notification.getCause());
        cachedFile.markRemoved();
    }

    private int weigh(Path cacheKey, CachedFile cachedFile) {
        return cachedFile.kibiBytes;
    }

    @Override
    public void close() throws IOException {
        cache.invalidateAll();

        Arrays.stream(cacheFileIO.listStatus(cachePath))
                .filter(f -> f.getPath().getName().startsWith(PREFIX))
                .forEach(f -> cacheFileIO.deleteQuietly(f.getPath()));
        LOG.info("Closed file cache in {}.", cachePath);
    }

    /** Cached local file. */
    public static class CachedFile {

        public final long size;
        public final Path path;
        public final int kibiBytes;

        private final Runnable deleteAction;
        private final AtomicLong refCount = new AtomicLong();
        private final AtomicBoolean removed = new AtomicBoolean(false);
        private final AtomicBoolean deleted = new AtomicBoolean(false);

        public CachedFile(long size, Path path, Runnable deleteAction) {
            this.size = size;
            this.path = path;
            this.kibiBytes = toKibiBytes(size);
            this.deleteAction = deleteAction;
        }

        public CachedFile incRef() {
            refCount.incrementAndGet();
            return this;
        }

        public void decRef() {
            long references = refCount.decrementAndGet();
            if (references < 0) {
                throw new IllegalStateException("Reference count becomes negative for " + path);
            }
            if (references == 0) {
                tryDelete();
            }
        }

        public boolean isUsing() {
            return refCount.get() > 0;
        }

        private void markRemoved() {
            removed.set(true);
            tryDelete();
        }

        private void tryDelete() {
            if (removed.get() && refCount.get() == 0 && deleted.compareAndSet(false, true)) {
                deleteAction.run();
            }
        }

        @Override
        public String toString() {
            return "CachedFile{"
                    + "size="
                    + size
                    + ", path="
                    + path
                    + ", kibiBytes="
                    + kibiBytes
                    + ", refCount="
                    + refCount
                    + '}';
        }
    }

    public static int toKibiBytes(long size) {
        long kibiBytes = size >> 10;
        if (kibiBytes > Integer.MAX_VALUE) {
            throw new RuntimeException("The file is too big: " + MemorySize.ofKibiBytes(kibiBytes));
        }
        return (int) kibiBytes;
    }
}
