package org.jumbodb.connector.hadoop.configuration;

import org.jumbodb.connector.hadoop.index.output.AbstractIndexMapper;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class JumboCustomImportJob extends BaseJumboImportJob {
    private List<Class<? extends AbstractIndexMapper>> mapper = new LinkedList<Class<? extends AbstractIndexMapper>>();

    public List<Class<? extends AbstractIndexMapper>> getMapper() {
        return mapper;
    }

    public void setMapper(List<Class<? extends AbstractIndexMapper>> mapper) {
        this.mapper = mapper;
    }

    public void addMapper(Class<? extends AbstractIndexMapper> aClass) {
        mapper.add(aClass);
    }

    @Override
    public String toString() {
        return "JumboCustomImportJob{" +
                "mapper=" + mapper +
                '}';
    }
}
