package core.akka;

import java.io.File;
import java.util.Set;

/**
 * User: carsten
 * Date: 2/5/13
 * Time: 2:52 PM
 */
public class SearchIndexFileQueryMessage {
    private final File indexFile;
    private final Set<Integer> hashes;

    public SearchIndexFileQueryMessage(File indexFile, Set<Integer> hashes) {
        this.indexFile = indexFile;
        this.hashes = hashes;
    }

    public File getIndexFile() {
        return indexFile;
    }

    public Set<Integer> getHashes() {
        return hashes;
    }
}
