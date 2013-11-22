package org.jumbodb.connector.importer

import org.apache.commons.io.IOUtils
import org.apache.commons.lang.RandomStringUtils
import org.apache.commons.lang.math.RandomUtils
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class MonitorCountingOutputStreamSpec extends Specification {
    def "test copy rate"() {
        setup:
        def bos = new ByteArrayOutputStream()
        def data = RandomStringUtils.randomAscii(204800).getBytes("UTF-8")  // 200 KB
        def cos = new MonitorCountingOutputStream(bos, 1000)
        when:
        cos.write(data)
        then: "data is not added because wait time of 1000ms not yet reached"
        cos.getNotMeasuredBytes() == 204800l
        cos.getMeasuredBytes() == 0l
        cos.getRateInBytesPerSecond() == 0l
        cos.getByteCount() == 204800l
        cos.getCount() == 204800l
        when: "waiting for 1000ms so the new data will be used"
        Thread.sleep(1000l)
        cos.write(data)
        then:
        cos.getMeasuredBytes() == 409600l
        cos.getNotMeasuredBytes() == 0l
        cos.getRateInBytesPerSecond() > 100l // can not clearly defined, depends on machine speed
        cos.getRateInBytesPerSecond() <= 409600l // must be smaller or equals because thats the maximum
        cos.getByteCount() == 409600l
        cos.getCount() == 409600l
    }
}
