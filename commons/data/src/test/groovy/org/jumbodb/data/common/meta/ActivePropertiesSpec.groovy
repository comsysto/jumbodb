package org.jumbodb.data.common.meta

import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class ActivePropertiesSpec extends Specification {
    def "test write and load configuration"() {
        setup:
        def file = File.createTempFile("test", "file")
        when:
        ActiveProperties.writeActiveFile(file, "my_version")
        def version = ActiveProperties.getActiveDeliveryVersion(file)
        then:
        version == "my_version"
        cleanup:
        file.delete()
    }
}
