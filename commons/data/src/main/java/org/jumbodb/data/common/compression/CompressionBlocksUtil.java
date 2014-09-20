package org.jumbodb.data.common.compression;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class CompressionBlocksUtil {
    private static final long CHUNK_HEADER_SIZE = 8 + 8 + 4 + 4;

    public static Blocks getBlocksByFile(File compressedFile) {
        String blocksFileName = compressedFile.getAbsolutePath() + ".blocks";
        File chunkFile = new File(blocksFileName);
        FileInputStream blocksFis = null;
        DataInputStream blocksDis = null;
        try {
            blocksFis = new FileInputStream(blocksFileName);
            blocksDis = new DataInputStream(new BufferedInputStream(blocksFis));
            long length = blocksDis.readLong();
            long datasets = blocksDis.readLong();
            int snappyChunkSize = blocksDis.readInt();
            int numberOfChunks = (int) (chunkFile.length() - CHUNK_HEADER_SIZE) / 4;
            List<Integer> snappyChunks = buildBlocks(blocksDis, numberOfChunks);
            return new Blocks(length, datasets, snappyChunkSize, numberOfChunks, snappyChunks);

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(blocksDis);
            IOUtils.closeQuietly(blocksFis);
        }
    }

    private static List<Integer> buildBlocks(DataInputStream blocksDis, long numberOfBlocks) throws IOException {
        List<Integer> compressionBlocks = new ArrayList<Integer>((int) numberOfBlocks);
        // - 8 is length value, 4 is the chunksize
        blocksDis.readInt(); // remove version chunk
        for (int i = 0; i < numberOfBlocks; i++) {
            compressionBlocks.add(blocksDis.readInt());
        }
        return compressionBlocks;
    }
}
