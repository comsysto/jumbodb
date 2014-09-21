package org.jumbodb.connector.hadoop.configuration;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class JumboCustomImportJob extends BaseJumboImportJob {
    private List<JumboCustomIndexJob> indexJobs = new LinkedList<JumboCustomIndexJob>();

    public List<JumboCustomIndexJob> getIndexJobs() {
        return indexJobs;
    }

    public void setIndexJobs(List<JumboCustomIndexJob> indexJobs) {
        this.indexJobs = indexJobs;
    }

    public void addIndexJob(JumboCustomIndexJob indexJob) {
        indexJobs.add(indexJob);
    }
}
