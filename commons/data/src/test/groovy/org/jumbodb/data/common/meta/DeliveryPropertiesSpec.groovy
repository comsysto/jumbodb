package org.jumbodb.data.common.meta

import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class DeliveryPropertiesSpec extends Specification {
    def "test write and load configuration"() {
        setup:
        def file = File.createTempFile("test", "file")
        def metaToWrite = new DeliveryProperties.DeliveryMeta("my_version", "some infos", new Date(480000l), "source path", "my_test_strategy")
        when:
        DeliveryProperties.write(file, metaToWrite)
        def meta = DeliveryProperties.getDeliveryMeta(file)
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
        def metaToWrite = new DeliveryProperties.DeliveryMeta("my_version", "some infos", new Date(480000l), "source path", "my_test_strategy")
        when:
        DeliveryProperties.write(file, metaToWrite)
        def date = DeliveryProperties.getDate(file)
        then:
        date == new Date(480000l)
        cleanup:
        file.delete()
    }

    def "test getStrategy"() {
        setup:
        def file = File.createTempFile("test", "file")
        def metaToWrite = new DeliveryProperties.DeliveryMeta("my_version", "some infos", new Date(480000l), "source path", "my_test_strategy")
        when:
        DeliveryProperties.write(file, metaToWrite)
        def strategy = DeliveryProperties.getStrategy(file)
        then:
        strategy == "my_test_strategy"
        cleanup:
        file.delete()
    }
}