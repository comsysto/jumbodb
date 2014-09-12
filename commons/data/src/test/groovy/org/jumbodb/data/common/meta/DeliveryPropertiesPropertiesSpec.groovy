package org.jumbodb.data.common.meta

import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class DeliveryPropertiesPropertiesSpec extends Specification {
    def "test write and load configuration"() {
        setup:
        def file = File.createTempFile("test", "file")
        def metaToWrite = new DeliveryProperties.DeliveryMeta("delivery_key", "version", "2012-12-12 12:12:12", "info")
        when:
        DeliveryProperties.write(file, metaToWrite)
        def meta = DeliveryProperties.getDeliveryMeta(file)
        then:
        meta.getDate() == "2012-12-12 12:12:12"
        meta.getInfo()  == "info"
        meta.getDelivery()  == "delivery_key"
        meta.getVersion()  == "version"
        cleanup:
        file.delete()
    }
}
