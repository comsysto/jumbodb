package org.jumbodb.connector.hadoop.test;

import java.io.*;

/**
 * User: carsten
 * Date: 11/6/12
 * Time: 2:34 PM
 */
public class VerifyFile {
  //  private static long count = 1;

    public static void main(String args[]) throws Exception {
        File file = new File("/Users/carsten/smhadoop/input/newindex");
        File[] idxes = file.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.getName().endsWith(".idx");
            }
        });
        for (File idx : idxes) {
            System.out.println(idx.getName());
            verify(idx);
            System.out.println("------");

        }
    }

    private static void verify(File file) throws IOException {
        DataInputStream br = new DataInputStream(new FileInputStream(file));
        System.out.println("==> First hash: " + br.readInt());
        br.readInt();
        br.readLong();
        //     System.out.println(br.readInt());
        //     System.out.println(br.readLong());

        int lastHash = Integer.MIN_VALUE;
        int fileHash = 0;
        long offset = 0;
        while(br.available() > 0) {
           int currentHash = br.readInt();
           fileHash = br.readInt();
           offset = br.readLong();
           if(currentHash < lastHash) {
               System.err.println("Problem: " + currentHash);
           }
           lastHash = currentHash;
         //   count++;
        }
//        System.out.println(fileHash);
//        System.out.println(offset);
//        System.out.println("count " + count);
        System.out.println("=> Last hash: " + lastHash);
        br.close();
    }


}
