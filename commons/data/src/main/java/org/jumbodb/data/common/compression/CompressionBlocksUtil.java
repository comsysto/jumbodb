package org.jumbodb.data.common.compression;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.UnhandledException;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class CompressionBlocksUtil {

    public static Blocks getBlocksByFile(File compressedFile) {
        String blocksFileName = compressedFile.getAbsolutePath() + ".blocks";
        File blockFile = new File(blocksFileName);
        FileInputStream blocksFis = null;
        DataInputStream blocksDis = null;
        try {
            blocksFis = new FileInputStream(blockFile);
            blocksDis = new DataInputStream(new BufferedInputStream(blocksFis));
            long length = blocksDis.readLong();
            long datasets = blocksDis.readLong();
            int compressionBlockSize = blocksDis.readInt();
            int numberOfBlocks = blocksDis.readInt();
            List<Integer> blocks = buildBlocks(blocksDis, numberOfBlocks);
            return new Blocks(length, datasets, compressionBlockSize, numberOfBlocks, blocks);

        } catch (FileNotFoundException e) {
            throw new UnhandledException(e);
        } catch (IOException e) {
            throw new UnhandledException(e);
        } finally {
            IOUtils.closeQuietly(blocksDis);
            IOUtils.closeQuietly(blocksFis);
        }
    }

    private static List<Integer> buildBlocks(DataInputStream blocksDis, long numberOfBlocks) throws IOException {
        List<Integer> compressionBlocks = new ArrayList<Integer>((int) numberOfBlocks);
        for (int i = 0; i < numberOfBlocks; i++) {
            compressionBlocks.add(blocksDis.readInt());
        }
        return compressionBlocks;
    }
}
