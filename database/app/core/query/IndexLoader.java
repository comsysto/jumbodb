package core.query;

import com.google.common.collect.HashMultimap;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.SuffixFileFilter;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * User: carsten
 * Date: 2/6/13
 * Time: 2:54 PM
 */
public class IndexLoader {
    // CARSTEN remove
    public static final String temporaryDefaultDeliveryKey = "first_delivery";

    public static Map<String, DataCollection> loadIndex(File dataPath, File indexPath) {
        Map<String, DataCollection> result = new HashMap<String, DataCollection>();
        File[] dataCollectionDirectories = dataPath.listFiles((FileFilter) DirectoryFileFilter.INSTANCE);
        if(dataCollectionDirectories == null) {
            return result;
        }
        for (File dataCollectionFolder  : dataCollectionDirectories) {
            String collectionName = dataCollectionFolder.getName();
            // CARSTEN fix this later temporaryDefaultDeliveryKey
            String dataDeliveryKeyFolder = dataCollectionFolder.getAbsolutePath() + "/" + temporaryDefaultDeliveryKey + "/";
            Properties activeProps = loadProperties(new File(dataDeliveryKeyFolder + "active.properties"));
            String deliveryVersion = activeProps.getProperty("deliveryVersion");

            String dataDeliveryVersionFolder = dataDeliveryKeyFolder + deliveryVersion + "/";
            String indexDeliveryVersionFolder = indexPath.getAbsolutePath() + "/" + collectionName + "/" + temporaryDefaultDeliveryKey + "/" + deliveryVersion + "/";
            DataCollection dataCollection = createDataCollection(new File(indexDeliveryVersionFolder), new File(dataDeliveryVersionFolder));
            result.put(collectionName, dataCollection);
        }
        return result;

//        Map<String, DataCollection> result = new HashMap<String, DataCollection>();
//        File[] files = indexPath.listFiles((FileFilter) DirectoryFileFilter.INSTANCE);
//        if(files == null) {
//            return result;
//        }
//        for (File collectionIndexFolder  : files) {
//            String collectionName = collectionIndexFolder.getName();
//            DataCollection dataCollection = createDataCollection(collectionIndexFolder, new File(dataPath.getAbsolutePath() + "/" + collectionName + "/"));
//            result.put(collectionName, dataCollection);
//        }
//        return result;
    }

    public static Properties loadProperties(File file) {
        Properties props = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            props.load(fis);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(fis);
        }
        return props;

    }

    private static DataCollection createDataCollection(File collectionIndexFolder, File collectionDataFolder) {
        File[] indexFolders = collectionIndexFolder.listFiles((FileFilter) DirectoryFileFilter.INSTANCE);
        HashMultimap<String, IndexFile> resIndexFiles = HashMultimap.create();
        Map<Integer, File> resDataFiles = new HashMap<Integer, File>();
        for (File indexFolder : indexFolders) {
            File[] indexFiles = indexFolder.listFiles((FilenameFilter) new SuffixFileFilter(".odx"));
            for (File indexFile : indexFiles) {
                if(indexFile.length() > 0) {
                    resIndexFiles.put(indexFolder.getName(), createIndexFileDescription(indexFile));
                }
            }
        }
        File[] dataFiles = collectionDataFolder.listFiles((FilenameFilter) new NotFileFilter(new SuffixFileFilter(".properties")));
        for (File dataFile : dataFiles) {
            resDataFiles.put(dataFile.getName().hashCode(), dataFile);
        }
        return new DataCollection(resIndexFiles.asMap(), resDataFiles);
    }


    private static IndexFile createIndexFileDescription(File indexFile) {
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(indexFile, "r");
            int fromHash = raf.readInt();
            raf.seek(raf.length() - 16);
            int toHash = raf.readInt();
            return new IndexFile(fromHash, toHash, indexFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(raf);
        }
    }
}
