package org.jumbodb.common.util.file

import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll
/**
 * @author Ulf Gitschthaler
 */
class DirectoryUtilSpec extends Specification {

    @Shared
    def testDir

    def setupSpec(){
        testDir = new File(System.properties["java.io.tmpdir"], "test_dir")
        testDir.mkdirs()
    }

    def cleanupSpec(){
        testDir.deleteDir()
    }

    def "filter data files"(){
        expect:
        filesToCreate.every{f -> new File(testDir, f).createNewFile()}
        def dataFiles = DirectoryUtil.listDataFiles(testDir)
        filesToList.size() == dataFiles.size()
        dataFiles.collect{f -> f.name}.containsAll(filesToList)
        where:
        filesToCreate = [
                "part-r-00000",
                "part-r-12344",
                "part-r-99999",
                "delivery.properties",
                "_SUCCESS",
                "_SUCCESS.chunks.snappy"
        ]
        filesToList = [
                "part-r-00000",
                "part-r-12344",
                "part-r-99999"
        ]
    }

    @Ignore("this tests does not work on Windows") // TODO this tests does not work on Windows
    @Unroll("concatenation results in #expectedResult")
    def "concatenate file paths"(){
        expect:
        expectedResult == DirectoryUtil.concatenatePaths(folders).absolutePath
        where:
        folders                             | expectedResult
        ""                                  | new File("").absolutePath
        "foo"                               | "${new File("").absolutePath}/foo"
        "/var/tmp"                          | "/var/tmp"
        ["/var", "tmp", "bla"] as String[]  | "/var/tmp/bla"
        ["foo", "bar"] as String []         | "${new File("").absolutePath}/foo/bar"
    }
}