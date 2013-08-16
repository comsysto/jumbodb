package org.jumbodb.data.common.meta

import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class PropertiesHelperSpec extends Specification {
    def "test loadProperties"() {
        setup:
        def file = File.createTempFile("test", "file")
        def fos = new FileOutputStream(file)
        def props = new Properties()
        props.setProperty("test", "value")
        props.setProperty("test2", "value2")
        props.store(fos, "no comment")
        fos.close()
        when:
        def result = PropertiesHelper.loadProperties(file)
        then:
        result.getProperty("test") == "value"
        result.getProperty("test2") == "value2"
        cleanup:
        file.delete()
    }
}
