package org.jumbodb.common.util.file;

import org.apache.commons.lang.ArrayUtils;

import java.io.File;
import java.io.FilenameFilter;

/**
 * @author Ulf Gitschthaler
 */
public class DirectoryUtil {

    public static File concatenatePaths(String ... pathFragments) {
        if (ArrayUtils.isEmpty(pathFragments)) {
            return null;
        }
        File result = new File(pathFragments[0]);
        if (ArrayUtils.getLength(pathFragments) == 1){
            return result;
        }
        for (int i = 1; i < pathFragments.length; i++) {
            result = new File(result, pathFragments[i]);
        }
        return result;
    }

    public static File[] listDataFiles(File dataFilesDir) {
        if (dataFilesDir == null || !dataFilesDir.exists()) {
            return null;
        }
        return dataFilesDir.listFiles(new DataFileFilter());
    }


    private static class DataFileFilter implements FilenameFilter {
        @Override
        public boolean accept(File dir, String name) {
            return !(name.endsWith(".properties") || name.contains("_SUCCESS") || name.endsWith(".snappy"));
        }
    }
}

