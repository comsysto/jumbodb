package org.jumbodb.database.service.query.index.basic.numeric;

/**
 * @author Carsten Hufe
 */
public interface QueryValueRetriever {
    <T> T getValue();
}
