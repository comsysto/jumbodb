package org.jumbodb.database.service.query.definition

import org.jumbodb.data.common.meta.ActiveProperties
import org.jumbodb.data.common.meta.CollectionProperties
import org.jumbodb.data.common.meta.IndexProperties
import spock.lang.Specification

import java.text.SimpleDateFormat

/**
 * @author Carsten Hufe
 */
class CollectionDefinitionLoaderSpec extends Specification {
    def rootPath = File.createTempFile("test", "file").getParentFile()
    def dataPath = new File(rootPath.absolutePath + "/data/")
    def indexPath = new File(rootPath.absolutePath + "/index/")

    def createDataCollectionVersion(collection, chunkKey, version, date) {
        def versionPath = dataPath.absolutePath + "/" + chunkKey + "/" + version + "/" + collection + "/"
        new File(versionPath).mkdirs()
        new File(versionPath + "part0001.snappy").createNewFile()
        new File(versionPath + "part0001.sha1").createNewFile()
        new File(versionPath + "part0001.md5").createNewFile()
        new File(versionPath + "part0001.snappy.chunks").createNewFile()
        new File(versionPath + "part0002.snappy").createNewFile()
        new File(versionPath + "part0002.sha1").createNewFile()
        new File(versionPath + "part0002.md5").createNewFile()
        new File(versionPath + "part0002.snappy.chunks").createNewFile()
        def metaData = new CollectionProperties.CollectionMeta(date, "Data imported from", "TEST_STRATEGY")
        CollectionProperties.write(new File(versionPath + "/" + CollectionProperties.DEFAULT_FILENAME), metaData)
    }

    def writeActiveProperties(chunkKey, version) {
        def chunkKeyPath = dataPath.absolutePath + "/" + chunkKey + "/"
        ActiveProperties.writeActiveFile(new File(chunkKeyPath + "/" + ActiveProperties.DEFAULT_FILENAME), version);
    }

    def createIndexCollectionVersion(collection, chunkKey, version) {
        def index1Path = indexPath.absolutePath + "/" + chunkKey + "/" + version + "/"  + collection + "/index1/"
        def index2Path = indexPath.absolutePath + "/" + chunkKey + "/" + version + "/"  + collection + "/index2/"
        new File(index1Path).mkdirs()
        new File(index2Path).mkdirs()

        new File(index1Path + "part0001.idx").createNewFile()
        new File(index1Path + "part0001.idx").createNewFile()
        new File(index1Path + "part0002.idx").createNewFile()
        new File(index1Path + "part0002.idx.chunks").createNewFile()

        new File(index2Path + "part0001.idx").createNewFile()
        new File(index2Path + "part0001.idx.chunks").createNewFile()
        new File(index2Path + "part0002.idx").createNewFile()
        new File(index2Path + "part0002.idx.chunks").createNewFile()

        def indexMeta1 = new IndexProperties.IndexMeta("my_date", "index1", "INDEX1_STRATEGY", "some index source field1")
        IndexProperties.write(new File(index1Path + "/" + IndexProperties.DEFAULT_FILENAME), indexMeta1)
        def indexMeta2 = new IndexProperties.IndexMeta("my_date", "index2", "INDEX2_STRATEGY", "some index source field2")
        IndexProperties.write(new File(index2Path + "/" + IndexProperties.DEFAULT_FILENAME), indexMeta2)
    }

    def "verify loaded data structure"() {
        setup:
        createDataCollectionVersion("testCollection1", "firstChunk", "version1", "2012-01-01")
        createDataCollectionVersion("testCollection1", "firstChunk", "version2", "2012-01-02")
        createIndexCollectionVersion("testCollection1", "firstChunk", "version1")
        createIndexCollectionVersion("testCollection1", "firstChunk", "version2")
        writeActiveProperties("firstChunk", "version2")
        when:
        def cd = CollectionDefinitionLoader.loadCollectionDefinition(dataPath, indexPath)
        then:
        cd.getCollections().size() == 1
        cd.getCollections().contains("testCollection1")
        when:
        def chunks = cd.getChunks("testCollection1")
        then:
        chunks.size() == 1
        chunks[0].collection == "testCollection1"
        chunks[0].chunkKey == "firstChunk"
        chunks[0].dataStrategy == "TEST_STRATEGY"
        when:
        def indexes = chunks[0].indexes
        then:
        indexes.size() == 2
        indexes[0].name == "index1"
        indexes[0].path == new File(indexPath.absolutePath + "/firstChunk/version2/testCollection1/index1/")
        indexes[0].strategy == "INDEX1_STRATEGY"
        indexes[1].name == "index2"
        indexes[1].path == new File(indexPath.absolutePath + "/firstChunk/version2/testCollection1/index2/")
        indexes[1].strategy == "INDEX2_STRATEGY"

        when:
        def dataFiles = chunks[0].dataFiles
        then:
        dataFiles.size() == 2
        dataFiles.get("part0001.snappy".hashCode()) == new File(dataPath.absolutePath + "/firstChunk/version2/testCollection1/part0001.snappy")
        dataFiles.get("part0002.snappy".hashCode()) == new File(dataPath.absolutePath + "/firstChunk/version2/testCollection1/part0002.snappy")
        cleanup:
        rootPath.delete()
    }
}
