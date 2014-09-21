package org.jumbodb.data.common.lz4;

import com.google.common.io.NullOutputStream;
import org.apache.commons.io.IOUtils;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;

public class Benchmark {
    public static void main(String[] args) throws Exception {
//        snappyCompress();
//        Thread.sleep(10000);
//        snappyDecompress();
//        Thread.sleep(10000);
//        lz4compress();
//        Thread.sleep(10000);
        lz4decompress();
//
//        lz4decompressSkipDebug();
    }

    private static void snappyDecompress() throws IOException {
        long start = System.currentTimeMillis();
        FileInputStream fis = new FileInputStream("c:/Development/sample-0002.json.snappy");
        BufferedInputStream bis = new BufferedInputStream(fis);
        SnappyInputStream lz4In = new SnappyInputStream(bis);
//        FileOutputStream fos = new FileOutputStream("c:/Development/sample-0002.json.snappy.dec");
//        BufferedOutputStream bos = new BufferedOutputStream(fos);
        OutputStream bos = new NullOutputStream();
        IOUtils.copy(lz4In, bos);
        lz4In.close();
        bis.close();
        fis.close();
        bos.close();
//        fos.close();
        System.out.println("Snappy decompress: " + (System.currentTimeMillis() - start));
    }

    private static void snappyCompress() throws IOException {
        long start = System.currentTimeMillis();
        FileInputStream fis = new FileInputStream("c:/Development/data/twitter/input_big/sample-0002.json");
        BufferedInputStream bis = new BufferedInputStream(fis);
        FileOutputStream fos = new FileOutputStream("c:/Development/sample-0002.json.snappy");
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        SnappyOutputStream lzout = new SnappyOutputStream(bos);
        IOUtils.copy(bis, lzout);
        bis.close();
        fis.close();
        lzout.close();
        bos.close();
        fos.close();
        System.out.println("Snappy compress: " + (System.currentTimeMillis() - start));
    }

    private static void lz4decompress() throws IOException {
        long start = System.currentTimeMillis();
        FileInputStream fis = new FileInputStream("c:/Development/lz4neu");
        BufferedInputStream bis = new BufferedInputStream(fis);
        LZ4BlockInputStream lz4In = new LZ4BlockInputStream(bis);
        FileOutputStream fos = new FileOutputStream("c:/Development/lz4test.dec");
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        IOUtils.copy(lz4In, bos);
        lz4In.close();
        bis.close();
        fis.close();
        bos.close();
//        fos.close();
        System.out.println("LZ4 decompress: " + (System.currentTimeMillis() - start));
    }

    private static void lz4decompressSkipDebug() throws IOException {
        long start = System.currentTimeMillis();
        FileInputStream fis = new FileInputStream("C:\\Users\\Carsten\\jumbodb\\data\\first_delivery3\\b9ead752-b144-4434-b920-67fa6ed3b8af\\twitter_lz4\\part-r-00000.lz4");
        BufferedInputStream bis = new BufferedInputStream(fis);
        LZ4BlockInputStream lz4In = new LZ4BlockInputStream(bis);
        DataInputStream dis = new DataInputStream(lz4In);
        System.out.println("skip " + dis.skip(1024 * 1024));
        byte[] b = new byte[1000];
        dis.readFully(b);
        System.out.println(new String(b));
        dis.close();
        lz4In.close();
        bis.close();
        fis.close();
        System.out.println("LZ4 decompress: " + (System.currentTimeMillis() - start));
    }

    private static void lz4decompressDebug() throws IOException {
        long start = System.currentTimeMillis();
        FileInputStream fis = new FileInputStream("C:\\Users\\Carsten\\jumbodb\\data\\first_delivery\\6bc18bb6-a036-4366-afad-b883eb92729b\\twitter_lz4\\part-r-00000.lz4");
        LZ4BlockInputStream lz4In = new LZ4BlockInputStream(fis);
//        BufferedInputStream bis = new BufferedInputStream(lz4In);
        DataInputStream dis = new DataInputStream(lz4In);
        int i;
        int count = 0;
        int sum = 0;
        while((i = dis.readInt()) != -1){
            if (i > 1000) {
                System.out.println("error " + i + " / " + count + " / " + sum);
            }
            dis.readFully(new byte[i]);
            count++;
            sum += i;
        }
        dis.close();
        lz4In.close();
//        bis.close();
        fis.close();
        System.out.println("LZ4 decompress: " + (System.currentTimeMillis() - start));
    }

    private static void lz4compress() throws IOException {
        long start = System.currentTimeMillis();
        FileInputStream fis = new FileInputStream("c:/Development/data/twitter/input_big/sample-0002.json");
        BufferedInputStream bis = new BufferedInputStream(fis);
        FileOutputStream fos = new FileOutputStream("c:/Development/sample-0002.json.lz4");
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        LZ4BlockOutputStream lzout = new LZ4BlockOutputStream(bos, 32768);
        IOUtils.copy(bis, lzout);
        bis.close();
        fis.close();
        lzout.close();
        bos.close();
        fos.close();
        System.out.println("LZ4 compress: " + (System.currentTimeMillis() - start));
    }


//    }
}
