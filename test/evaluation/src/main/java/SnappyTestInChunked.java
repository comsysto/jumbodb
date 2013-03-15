import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyCodec;

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
public class SnappyTestInChunked {
    public static void main(String[] args) throws Exception {
        int chunkSize = 512 * 1024;
        String s = "/Users/carsten/bla512k.meta.snappy";
        File metaFile = new File(s);
        long start = System.currentTimeMillis();
        FileInputStream metaFis = new FileInputStream(metaFile);
        DataInputStream metaDis = new DataInputStream(new BufferedInputStream(metaFis));
        metaDis.readInt(); // skip meta length
        long chunks = metaFile.length() / 8;
        long toSkip = 32l * 1024l * 1024l * 1024l;
        long compressedToSkip = 0l;
        long currentUncompressed = 0l;
        for(int i = 0; i < 100000; i++) {
            int l = metaDis.readInt();
//            System.out.println("meta l " + l);
            compressedToSkip += l + 4; // int len
            currentUncompressed += chunkSize;
        }
        currentUncompressed -= chunkSize;

        FileInputStream fis = new FileInputStream("/Users/carsten/bla512k.snappy");
//        BufferedInputStream bis = new BufferedInputStream(fis);
        SnappyCodec.readHeader(fis);
        fis.skip(compressedToSkip);

        byte[] len = new byte[4];
        fis.read(len);
        int i = ChunkSkipableSnappyInputStream.readInt(len, 0);
        System.out.println("i " + i);

//        ChunkSkipableSnappyInputStream sis = new ChunkSkipableSnappyInputStream(bis);
//        bis.skip(compressedToSkip);
        byte[] b = new byte[i];
        fis.read(b);
//        fis.read(len);
        System.out.println(new String(Snappy.uncompress(b)));
        System.out.println("Time: " + (System.currentTimeMillis() - start));
        metaDis.close();
//        sis.close();
//        bis.close();
        fis.close();
    }
}
