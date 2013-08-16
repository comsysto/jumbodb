package org.jumbodb.data.common.meta

import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class IndexPropertiesSpec extends Specification {
    def "test write and load configuration"() {
        setup:
        def file = File.createTempFile("test", "file")
        def metaToWrite = new IndexProperties.IndexMeta("my_version", new Date(480000l), "my_index_name", "my_test_strategy", "source fields")
        when:
        IndexProperties.write(file, metaToWrite)
        def meta = IndexProperties.getIndexMeta(file)
        then:
        meta.getDate() == new Date(480000l)
        meta.getDeliveryVersion()  == "my_version"
        meta.getIndexName()  == "my_index_name"
        meta.getIndexSourceFields()  == "source fields"
        meta.getStrategy()  == "my_test_strategy"
        cleanup:
        file.delete()
    }

    def "test getStrategy"() {
        setup:
        def file = File.createTempFile("test", "file")
        def metaToWrite = new IndexProperties.IndexMeta("my_version", new Date(480000l), "my_index_name", "my_test_strategy", "source fields")
        when:
        IndexProperties.write(file, metaToWrite)
        def strategy = IndexProperties.getStrategy(file)
        then:
        strategy == "my_test_strategy"
        cleanup:
        file.delete()
    }
}
