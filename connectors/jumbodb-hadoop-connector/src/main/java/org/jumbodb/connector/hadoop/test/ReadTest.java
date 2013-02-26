package org.jumbodb.connector.hadoop.test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * User: carsten
 * Date: 11/6/12
 * Time: 2:34 PM
 */
public class ReadTest {
    List<IndexFile> indexFiles = new ArrayList<IndexFile>();

    public ReadTest() throws IOException {
        initalize();
        System.out.println("Offset: " + findOffsetForCellId("14a22142224"));
    }

    private long findOffsetForCellId(String s) throws IOException {
        int searchHash = s.hashCode();
        RandomAccessFile file = findFile(searchHash);
        long length = file.length();
        long fromOffset = 0l;
        long startTime = System.currentTimeMillis();
        long toOffset = length;
        int steps = 0;
        while(toOffset - fromOffset != 16) {
            long current = ((toOffset - fromOffset) / 2) - (((toOffset - fromOffset) / 2) % 16) + fromOffset;
            file.seek(current);
            int currentHash = file.readInt();
            if(currentHash == searchHash) {
                // next optimization jump 100 back then 200, 400 bis hash not equal, dann wieder vorwaerts die richtigen suchen

                System.out.println("Steps: " + steps + " Time: " + (System.currentTimeMillis() - startTime) + "ms");
                int fileHash =  file.readInt();
                System.out.println("FileHash: " + fileHash);
                long offset = file.readLong();
                return offset;
            }
            else if(currentHash < searchHash) {
                fromOffset = current;
            }
            else {
                toOffset = current;
            }
            steps++;
        }
        return -1;
    }

    private RandomAccessFile findFile(int hash) {
        for (IndexFile indexFile : indexFiles) {
            if(hash >= indexFile.fromHash && hash <= indexFile.toHash) {
                // immer nur eine file, da ein hash immer in einer datei ist
                return indexFile.indexFile;
            }
        }
        return null;
    }

    private void initalize() throws IOException {
        File file = new File("/Users/carsten/smhadoop/input/newindex");
        File[] idxes = file.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.getName().contains("idx");
            }
        });

        for (File idx : idxes) {
            IndexFile f = new IndexFile();
            f.indexFile = new RandomAccessFile(idx, "r");
            f.fromHash = f.indexFile.readInt();
            f.indexFile.seek(f.indexFile.length() - 16);
            f.toHash = f.indexFile.readInt();
            indexFiles.add(f);
        }
    }


    public static void main(String args[]) throws IOException {
        new ReadTest();

    }

    public class IndexFile {
        int fromHash;
        int toHash;
        RandomAccessFile indexFile;
    }
}
