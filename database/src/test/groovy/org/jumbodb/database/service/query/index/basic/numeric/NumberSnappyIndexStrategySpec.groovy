package org.jumbodb.database.service.query.index.basic.numeric

import org.apache.commons.io.FileUtils
import org.jumbodb.database.service.query.index.integer.snappy.IntegerSnappyIndexStrategy
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class NumberSnappyIndexStrategySpec extends Specification {
    def "getSize"() {
        def folderStr = FileUtils.getTempDirectory().absolutePath + "/" + UUID.randomUUID().toString() + "/"
        def folder = new File(folderStr)
        FileUtils.forceMkdir(folder);
        new File(folderStr + "/test.idx").text = "Hello World"
        def strategy = new IntegerSnappyIndexStrategy()
        when:
        def size = strategy.getSize(folder)
        then:
        size == 11
        cleanup:
        folder.deleteDir()
    }
}
