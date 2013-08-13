package org.jumbodb.database.service.exporter

import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class ExportDeliveryCountOutputStreamSpec extends Specification {
    def "set copy rate in bytes uncompressed"() {
        setup:
        def exportDeliveryMock = Mock(ExportDelivery)
        def stream = new ExportDeliveryCountOutputStream(Mock(OutputStream), exportDeliveryMock)
        when:
        stream.onInterval(10000l, 50000l)
        then:
        1 * exportDeliveryMock.setCopyRateInBytesUncompressed(10000l);
        1 * exportDeliveryMock.addCurrentBytes(50000l);
    }
}
