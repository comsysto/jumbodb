package org.jumbodb.data.common.meta

import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class CollectionPropertiesSpec extends Specification {
    def "test write and load configuration"() {
        setup:
        def file = File.createTempFile("test", "file")
        def metaToWrite = new CollectionProperties.CollectionMeta("2012-12-12 12:12:12", "source path", "my_test_strategy", "info", "yyyy-MM-dd")
        when:
        CollectionProperties.write(file, metaToWrite)
        def meta = CollectionProperties.getCollectionMeta(file)
        then:
        meta.getDate() == "2012-12-12 12:12:12"
        meta.getInfo()  == "info"
        meta.getSourcePath()  == "source path"
        meta.getStrategy()  == "my_test_strategy"
        meta.getDateFormat()  == "yyyy-MM-dd"
        cleanup:
        file.delete()
    }

    def "test getStrategy"() {
        setup:
        def file = File.createTempFile("test", "file")
        def metaToWrite = new CollectionProperties.CollectionMeta("2012-12-12 12:12:12", "source path", "my_test_strategy", "info", "yyyy-MM-dd")
        when:
        CollectionProperties.write(file, metaToWrite)
        def strategy = CollectionProperties.getStrategy(file)
        then:
        strategy == "my_test_strategy"
        cleanup:
        file.delete()
    }
}
