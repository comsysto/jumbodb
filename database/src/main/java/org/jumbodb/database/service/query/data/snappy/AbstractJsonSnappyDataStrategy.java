package org.jumbodb.database.service.query.data.snappy;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.database.service.query.data.CollectionDataSize;

import java.io.*;

/**
 * Created by Carsten on 19.09.2014.
 */
public abstract class AbstractJsonSnappyDataStrategy extends DefaultDataStrategy {
    @Override
    public CollectionDataSize getCollectionDataSize(File dataFolder) {
        long compressedSize = FileUtils.sizeOfDirectory(dataFolder);
        long uncompressedSize = 0l;
        long datasets = 0l;
        FileFilter metaFiler = FileFilterUtils.makeFileOnly(FileFilterUtils.suffixFileFilter(".chunks"));
        File[] snappyChunks = dataFolder.listFiles(metaFiler);
        for (File snappyChunk : snappyChunks) {
            SnappyChunkSize sizeFromSnappyChunk = getSizeFromSnappyChunk(snappyChunk);
            uncompressedSize += sizeFromSnappyChunk.uncompressed;
            datasets += sizeFromSnappyChunk.datasets;
        }
        return new CollectionDataSize(datasets, compressedSize, uncompressedSize);
    }

    private SnappyChunkSize getSizeFromSnappyChunk(File snappyChunk) {
        FileInputStream fis = null;
        DataInputStream dis = null;
        try {
            fis = new FileInputStream(snappyChunk);
            dis = new DataInputStream(fis);
            SnappyChunkSize snappyChunkSize = new SnappyChunkSize();
            snappyChunkSize.uncompressed = dis.readLong();
            snappyChunkSize.datasets = dis.readLong();
            return snappyChunkSize;
        } catch (FileNotFoundException e) {
            throw new UnhandledException(e);
        } catch (IOException e) {
            throw new UnhandledException(e);
        } finally {
            IOUtils.closeQuietly(dis);
            IOUtils.closeQuietly(fis);
        }
    }

    private static class SnappyChunkSize {
        long uncompressed;
        long datasets;
    }
}
