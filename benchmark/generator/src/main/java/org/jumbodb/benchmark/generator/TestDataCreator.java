package org.jumbodb.benchmark.generator;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;

public class TestDataCreator {

    public void create(long numberOfFiles, long datasetsPerFile, long datasetSizeInByte) {
        for (int fileNo = 0; fileNo < numberOfFiles; fileNo++) {
            //1. Create Filename
            //2. Create File
            //3. Create OutputStream for file
            //4. Iterate datasetsPerFile
            //4.1 Generate dataset, e.g. { data : "XXXX" }
            //4.2 createStringOfLength()
        }

    }

    private String createStringOfLength(int length) {
        return RandomStringUtils.randomAlphanumeric(length);
    }
}
