package core.akka;

import java.io.File;
import java.util.Set;

/**
 * User: carsten
 * Date: 2/5/13
 * Time: 2:52 PM
 */
public class SearchIndexFileResultMessage {
    private final Set<FileOffset> fileOffsets;

    public SearchIndexFileResultMessage(Set<FileOffset> fileOffsets) {
        this.fileOffsets = fileOffsets;
    }

    public Set<FileOffset> getFileOffsets() {
        return fileOffsets;
    }
}
