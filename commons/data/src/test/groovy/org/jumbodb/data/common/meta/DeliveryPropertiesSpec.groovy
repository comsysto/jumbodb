package org.jumbodb.data.common.meta

import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class DeliveryPropertiesSpec extends Specification {
    def "test write and load configuration"() {
        setup:
        def file = File.createTempFile("test", "file")
        def metaToWrite = new CollectionProperties.CollectionMeta("my_version", "some infos", new Date(480000l), "source path", "my_test_strategy")
        when:
        CollectionProperties.write(file, metaToWrite)
        def meta = CollectionProperties.getCollectionMeta(file)
        then:
        meta.getDate() == new Date(480000l)
        meta.getDeliveryVersion()  == "my_version"
        meta.getInfo()  == "some infos"
        meta.getSourcePath()  == "source path"
        meta.getStrategy()  == "my_test_strategy"
        cleanup:
        file.delete()
    }

    def "test getDate"() {
        setup:
        def file = File.createTempFile("test", "file")
        def metaToWrite = new CollectionProperties.CollectionMeta("my_version", "some infos", new Date(480000l), "source path", "my_test_strategy")
        when:
        CollectionProperties.write(file, metaToWrite)
        def date = CollectionProperties.getDate(file)
        then:
        date == new Date(480000l)
        cleanup:
        file.delete()
    }

    def "test getStrategy"() {
        setup:
        def file = File.createTempFile("test", "file")
        def metaToWrite = new CollectionProperties.CollectionMeta("my_version", "some infos", new Date(480000l), "source path", "my_test_strategy")
        when:
        CollectionProperties.write(file, metaToWrite)
        def strategy = CollectionProperties.getStrategy(file)
        then:
        strategy == "my_test_strategy"
        cleanup:
        file.delete()
    }
}
