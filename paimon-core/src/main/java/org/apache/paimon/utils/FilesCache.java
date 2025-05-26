package org.apache.paimon.utils;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.MemorySize;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;

/** Cache based on local file. */
public class FilesCache<K> {

    private final Cache<K, CachedFile> cache;
    private final Function<K, CachedFile> loader;
    private final FileIO cachedFileIO;

    public FilesCache(MemorySize diskSize, Function<K, CachedFile> loader) {
        this.cache =
                Caffeine.newBuilder()
                        .weigher(this::weigh)
                        .maximumWeight(diskSize.getKibiBytes())
                        .executor(Runnable::run)
                        .removalListener(this::removeCallback)
                        .build();
        this.loader = loader;
    }

    public FileIO getCachedFileIO() {
        return cachedFileIO;
    }

    public CachedFile load(Path path, FileIO fileIO, FileIO localFileIO) throws IOException {
        synchronized (cache) {
            Path localPath = new Path(cacheDir, path.getName());
            try (SeekableInputStream inputs = fileIO.newInputStream(path);
                    PositionOutputStream output = localFileIO.newOutputStream(localPath, true)) {
                IOUtils.copyBytes(inputs, output);
            }
        }
    }

    public CachedFile get(K key) {
        return cache.get(key, loader);
    }

    private void removeCallback(K key, CachedFile cachedFile, RemovalCause cause) {
        if (cachedFile != null) {
            cachedFile.file.delete();
            System.out.println("local file " + cachedFile.file.getAbsolutePath() + " deleted!.");
        }
    }

    private int weigh(K cacheKey, CachedFile cachedFile) {
        return cachedFile.kibiBytes;
    }

    public static class CachedFile {
        public final long size;
        public final File file;
        public final int kibiBytes;

        public CachedFile(long size, File file) {
            this.size = size;
            this.file = file;
            this.kibiBytes = toKibiBytes(size);
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
