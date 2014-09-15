package org.jumbodb.database.service.query.index.common;

/**
 * @author Carsten Hufe
 */
public interface QueryValueRetriever {
    <T> T getValue();
}
