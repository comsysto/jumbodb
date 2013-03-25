package core.management;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import core.importer.ImportHelper;
import core.query.Restartable;

import java.io.File;

/**
 * User: carsten
 * Date: 3/22/13
 * Time: 2:12 PM
 */
public class StorageManagement {
    private File dataPath;
    private File indexPath;
    private Restartable queryServer;

    public StorageManagement(File dataPath, File indexPath, Restartable queryServer) {
        this.dataPath = dataPath;
        this.indexPath = indexPath;
        this.queryServer = queryServer;
    }

    public void deleteCompleteCollection(String collectionName) {
        // nothing to activate, because collection is away


        // TODO
    }

    public void deleteChunkedVersion(String chunkedDeliveryKey, String version) {
        // activate other collections if active

        // TODO
    }

    public void deleteCompleteDeliveryChunk(String deliveryChunk) {
        // nothing to activate because it's a chunk

        // TODO
    }

    public void deleteChunkedVersionInCollection(String collection, String chunkDeliveryKey, String version) {
        // activate another version in the same chunk and collection
    }

    public void activateChunkedVersionForAllCollections(String chunkedDeliveryKey, String version) {
        // nothing to deactivate because its the same file to be written
        // TODO

    }

    public void activateChunkedVersionInCollection(String collection, String chunkDeliveryKey, String version) {
        File activeDeliveryFile = getActiveDeliveryFile(collection, chunkDeliveryKey);
        activateDeliveryVersion(version, activeDeliveryFile);
    }


    public String getActiveDeliveryVersion(String collection, String chunkDeliveryKey) {
        return ImportHelper.getActiveDeliveryVersion(getActiveDeliveryFile(collection, chunkDeliveryKey));
    }

    private File getActiveDeliveryFile(String collection, String chunkDeliveryKey) {
        return new File(dataPath.getAbsolutePath() + "/" + collection + "/" + chunkDeliveryKey + "/active.properties");
    }

    private String findAppropriateInactiveVersionToActivate(String collection, String deliveryChunkKey) {
        // TODO
        return null;
    }

    private void delete(String file) {
        delete(new File(file));
    }

    private void activateDeliveryVersion(String version, File activeDeliveryFile) {
        // TODO
        System.out.println("Activate " + activeDeliveryFile.getAbsolutePath() + " => " + version);
//        ImportHelper.writeActiveFile(activeDeliveryFile, version);
    }


    private void delete(File file) {
        // TODO
        System.out.println("Delete: " + file.getAbsolutePath());
    }

    public static void main(String[] args) {
        StorageManagement sm = new StorageManagement(new File("/Users/carsten/workspaces/jumbodb/database/~/jumbodb/data"), new File("/Users/carsten/workspaces/jumbodb/database/~/jumbodb/index"), new RestartableServer());
        sm.activateChunkedVersionInCollection("de.catchment.aggregated.daily.sum.by_cell", "first_delivery", "my_new_version");

    }

    private static class RestartableServer implements Restartable {
        @Override
        public void restart() {
            System.out.println("Restart Server");
        }
    }
    // CARSTEN here some list methods

}
