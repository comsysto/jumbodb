
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;

/**
 * Created with IntelliJ IDEA.
 * User: carsten
 * Date: 3/14/13
 * Time: 2:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class SnappyTestInChunkedStream2 {

    public static void main(String[] args) throws Exception {
        int chunkSize = 512 * 1024;
        String s = "/Users/carsten/workspaces/jumbodb/database/~/jumbodb/data/de.catchment.aggregated.daily.sum.by_cell/first_delivery/36da3747-f7fb-406d-bfc1-101b6c2cf927/part-r-00000.chunks.snappy";
        File metaFile = new File(s);
        long start = System.currentTimeMillis();
        FileInputStream metaFis = new FileInputStream(metaFile);
        DataInputStream metaDis = new DataInputStream(new BufferedInputStream(metaFis));
        long length = metaDis.readLong();// skip meta length
        long chunks = (metaFile.length() - 8) / 4;
//        long toSkip = 13135649;
        long compressedToSkip = 0l;
        long currentUncompressed = 0l;
        int currentChunk = 0;
        for(int i = 0; i < 2; i++) {
            int l = metaDis.readInt();
//            System.out.println("meta l " + l);
            compressedToSkip += l + 4; // int len
            currentUncompressed += chunkSize;
            currentChunk++;
        }
//        currentUncompressed -= chunkSize;

        FileInputStream fis = new FileInputStream("/Users/carsten/workspaces/jumbodb/database/~/jumbodb/data/de.catchment.aggregated.daily.sum.by_cell/first_delivery/36da3747-f7fb-406d-bfc1-101b6c2cf927/part-r-00000");
//        BufferedInputStream bis = new BufferedInputStream(fis);


        byte[] b = new byte[1024];
        ChunkSkipableSnappyInputStream sis = new ChunkSkipableSnappyInputStream(fis);
//        sis.read(b);
        System.out.println("chunk " + sis.getCurrentChunk() + " chunk offset " + sis.getCurrentChunkOffset());
//        sis.skipCompressed(compressedToSkip);
        sis.read(b);
        System.out.println(new String(b, 0, 100));
//        System.out.println("Time: " + (System.currentTimeMillis() - start));
//        metaDis.close();
//        sis.close();
//        bis.close();         //495659539  495659549
        fis.close();
    }
}
