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

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.MemorySize;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FilesCache}. */
public class FilesCacheTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testDeleteEvictedFileWhenLastReferenceReleased() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path remoteDir = new Path(tempDir.resolve("remote").toUri());
        Path cacheDir = new Path(tempDir.resolve("cache").toUri());
        Path file1 = new Path(remoteDir, "file-1");
        Path file2 = new Path(remoteDir, "file-2");
        writeFile(fileIO, file1, 2048);
        writeFile(fileIO, file2, 2048);

        FilesCache filesCache = new FilesCache(MemorySize.ofKibiBytes(2), cacheDir, fileIO);

        FilesCache.CachedFile cachedFile1 = filesCache.get(file1, fileIO);
        assertThat(fileIO.exists(cachedFile1.path)).isTrue();

        FilesCache.CachedFile cachedFile2 = filesCache.get(file2, fileIO);
        filesCache.getCache().cleanUp();
        assertThat(fileIO.exists(cachedFile1.path)).isTrue();

        cachedFile1.decRef();
        assertThat(fileIO.exists(cachedFile1.path)).isFalse();

        cachedFile2.decRef();
        filesCache.close();
    }

    @Test
    public void testKeepRecentlyAccessedFileWhenCacheIsFull() throws Exception {
        CountingFileIO fileIO = new CountingFileIO();
        Path remoteDir = new Path(tempDir.resolve("remote-recent").toUri());
        Path cacheDir = new Path(tempDir.resolve("cache-recent").toUri());
        Path file1 = new Path(remoteDir, "file-1");
        Path file2 = new Path(remoteDir, "file-2");
        writeFile(fileIO, file1, 2048);
        writeFile(fileIO, file2, 2048);

        FilesCache filesCache = new FilesCache(MemorySize.ofKibiBytes(2), cacheDir, fileIO);

        for (int i = 0; i < 5; i++) {
            FilesCache.CachedFile hotFile = filesCache.get(file1, fileIO);
            hotFile.decRef();
        }
        assertThat(fileIO.inputStreamCount()).isEqualTo(1);

        FilesCache.CachedFile recentFile = filesCache.get(file2, fileIO);
        recentFile.decRef();
        filesCache.getCache().cleanUp();

        FilesCache.CachedFile recentFileAgain = filesCache.get(file2, fileIO);
        recentFileAgain.decRef();

        assertThat(fileIO.inputStreamCount()).isEqualTo(2);
        assertThat(filesCache.getCache().asMap()).containsKey(file2).doesNotContainKey(file1);

        filesCache.close();
    }

    private void writeFile(LocalFileIO fileIO, Path file, int size) throws IOException {
        try (PositionOutputStream output = fileIO.newOutputStream(file, false)) {
            output.write(new byte[size]);
        }
    }
}
