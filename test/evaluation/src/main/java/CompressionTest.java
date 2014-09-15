import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class CompressionTest {
    public static void main(String[] args) throws InterruptedException, IOException {
        System.out.println("Generating data");
        ByteArrayOutputStream testDataStream = new ByteArrayOutputStream();
        for (int i = 0; i < 512; i++) {
            System.out.println("Generating " + i);
            testDataStream.write(RandomStringUtils.randomAlphabetic(1024 * 1024).getBytes());
        }
        byte[] testData = testDataStream.toByteArray();
        System.out.println(testData.length);
        IOUtils.closeQuietly(testDataStream);
        System.out.println("Snappy Test");
        Thread.sleep(1000);
        long start = System.currentTimeMillis();
        ByteArrayInputStream snappyIs = new ByteArrayInputStream(testData);
        ByteArrayOutputStream snappyBos = new ByteArrayOutputStream();
        SnappyOutputStream snappyOutputStream = new SnappyOutputStream(snappyBos);
        IOUtils.copy(snappyIs, snappyOutputStream);
        IOUtils.closeQuietly(snappyOutputStream);
        IOUtils.closeQuietly(snappyBos);
        IOUtils.closeQuietly(snappyOutputStream);
        byte[] snappyCompressed = snappyBos.toByteArray();
        System.out.println("Snappy Time Compress: " + (System.currentTimeMillis() - start) + " Compressed bytes: " + snappyCompressed.length);
        snappyBos = null;
        snappyOutputStream = null;
        snappyOutputStream = null;
        Thread.sleep(1000);
        start = System.currentTimeMillis();
        ByteArrayInputStream snappyDecompressBis = new ByteArrayInputStream(snappyCompressed);
        SnappyInputStream snappyDecIs = new SnappyInputStream(snappyDecompressBis);
        IOUtils.copy(snappyDecIs, new ByteArrayOutputStream());
        System.out.println("Snappy Time Decompress: " + (System.currentTimeMillis() - start));
        IOUtils.closeQuietly(snappyDecIs);
        IOUtils.closeQuietly(snappyDecompressBis);
        snappyDecompressBis = null;
        snappyDecIs = null;
        snappyCompressed = null;

        System.out.println("LZ4 Test");
        Thread.sleep(1000);

        start = System.currentTimeMillis();
        ByteArrayInputStream lzIs = new ByteArrayInputStream(testData);
        ByteArrayOutputStream lzBos = new ByteArrayOutputStream();
        LZ4BlockOutputStream lzOutputStream = new LZ4BlockOutputStream(lzBos);
        IOUtils.copy(lzIs, lzOutputStream);
        IOUtils.closeQuietly(lzOutputStream);
        IOUtils.closeQuietly(lzBos);
        IOUtils.closeQuietly(lzIs);
        final byte[] lz4Compressed = lzBos.toByteArray();
        System.out.println("Lz Time: " + (System.currentTimeMillis() - start) + " Compressed bytes: " + lz4Compressed.length);

        lzBos = null;
        lzIs = null;
        lzOutputStream = null;
        Thread.sleep(1000);
        start = System.currentTimeMillis();
        ByteArrayInputStream lz4DecompressBis = new ByteArrayInputStream(lz4Compressed);
        LZ4BlockInputStream lz4DecIs = new LZ4BlockInputStream(lz4DecompressBis);
        IOUtils.copy(lz4DecIs, new ByteArrayOutputStream());
        System.out.println("LZ4 Time Decompress: " + (System.currentTimeMillis() - start));
        IOUtils.closeQuietly(lz4DecIs);
        IOUtils.closeQuietly(lz4DecompressBis);
        lz4DecompressBis = null;
        lz4DecIs = null;
    }


}
