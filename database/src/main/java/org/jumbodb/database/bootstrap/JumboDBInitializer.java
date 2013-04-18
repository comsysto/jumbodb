package org.jumbodb.database.bootstrap;

import org.jumbodb.database.service.configuration.JumboConfiguration;

import java.io.File;

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

        if(!dataPath.exists()) {
            dataPath.mkdirs();
        }

        if(!indexPath.exists()) {
            indexPath.mkdirs();
        }
    }
}