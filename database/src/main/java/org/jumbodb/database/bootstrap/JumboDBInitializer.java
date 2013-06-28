package org.jumbodb.database.bootstrap;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jumbodb.database.service.configuration.JumboConfiguration;

import java.io.File;
import java.io.IOException;

/**
 * @author ugitsch
 */
public class JumboDBInitializer {

    // set by spring
    private final JumboConfiguration jumboConfiguration;


    public JumboDBInitializer(JumboConfiguration jumboConfiguration) {
        this.jumboConfiguration = jumboConfiguration;
    }


    public void initialize(){

        createRequiredFolders();
    }

    private void createRequiredFolders() {

        File dataPath = jumboConfiguration.getDataPath();
        File indexPath = jumboConfiguration.getIndexPath();

        try {
            if(!dataPath.exists()) {
                FileUtils.forceMkdir(dataPath);
            }
            if(!indexPath.exists()) {
                FileUtils.forceMkdir(indexPath);
            }
        } catch (IOException e){
            throw new IllegalStateException("Unable to create necessary directories");
        }
    }
}