package org.jumbodb.database.service.query.data.lz4;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.database.service.query.data.CollectionDataSize;
import org.jumbodb.database.service.query.data.common.DefaultDataStrategy;

import java.io.*;

/**
 * Created by Carsten on 19.09.2014.
 */
public abstract class AbstractJsonLz4DataStrategy extends DefaultDataStrategy {
    @Override
    public CollectionDataSize getCollectionDataSize(File dataFolder) {
        long compressedSize = FileUtils.sizeOfDirectory(dataFolder);
        long uncompressedSize = 0l;
        long datasets = 0l;
        FileFilter metaFiler = FileFilterUtils.makeFileOnly(FileFilterUtils.suffixFileFilter(".chunks"));
        File[] lz4Chunks = dataFolder.listFiles(metaFiler);
        for (File lz4Chunk : lz4Chunks) {
            Lz4ChunkSize sizeFromLz4Chunk = getSizeFromLz4Chunk(lz4Chunk);
            uncompressedSize += sizeFromLz4Chunk.uncompressed;
            datasets += sizeFromLz4Chunk.datasets;
        }
        return new CollectionDataSize(datasets, compressedSize, uncompressedSize);
    }

    private Lz4ChunkSize getSizeFromLz4Chunk(File snappyChunk) {
        FileInputStream fis = null;
        DataInputStream dis = null;
        try {
            fis = new FileInputStream(snappyChunk);
            dis = new DataInputStream(fis);
            Lz4ChunkSize lz4ChunkSize = new Lz4ChunkSize();
            lz4ChunkSize.uncompressed = dis.readLong();
            lz4ChunkSize.datasets = dis.readLong();
            return lz4ChunkSize;
        } catch (FileNotFoundException e) {
            throw new UnhandledException(e);
        } catch (IOException e) {
            throw new UnhandledException(e);
        } finally {
            IOUtils.closeQuietly(dis);
            IOUtils.closeQuietly(fis);
        }
    }

    private static class Lz4ChunkSize {
        long uncompressed;
        long datasets;
    }
}
