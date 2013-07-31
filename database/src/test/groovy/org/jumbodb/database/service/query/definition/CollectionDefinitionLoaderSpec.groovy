package org.jumbodb.database.service.query.definition

import org.jumbodb.database.service.importer.ImportHelper
import org.jumbodb.database.service.importer.ImportMetaData
import org.jumbodb.database.service.importer.ImportMetaIndex

/**
 * @author Carsten Hufe
 */
class CollectionDefinitionLoaderSpec extends spock.lang.Specification {
    def rootPath = File.createTempFile("test", "file").getParentFile()
    def dataPath = new File(rootPath.absolutePath + "/data/")
    def indexPath = new File(rootPath.absolutePath + "/index/")

    def createDataCollectionVersion(collection, chunkKey, version) {
        def versionPath = dataPath.absolutePath + "/" + collection + "/" + chunkKey + "/" + version + "/"
        new File(versionPath).mkdirs()
        new File(versionPath + "part0001").createNewFile()
        new File(versionPath + "part0001.chunks.snappy").createNewFile()
        new File(versionPath + "part0002").createNewFile()
        new File(versionPath + "part0002.chunks.snappy").createNewFile()
        def metaData = new ImportMetaData(collection, chunkKey, version, "TEST_STRATEGY", "Data imported from", "Some info")
        ImportHelper.writeDataDeliveryProperties(metaData, new File(versionPath + "/delivery.properties"))
    }

    def writeActiveProperties(collection, chunkKey, version) {
        def chunkKeyPath = dataPath.absolutePath + "/" + collection + "/" + chunkKey + "/"
        ImportHelper.writeActiveFile(new File(chunkKeyPath + "/active.properties"), version);
    }

    def createIndexCollectionVersion(collection, chunkKey, version) {
        def index1Path = indexPath.absolutePath + "/" + collection + "/" + chunkKey + "/" + version + "/index1/"
        def index2Path = indexPath.absolutePath + "/" + collection + "/" + chunkKey + "/" + version + "/index2/"
        new File(index1Path).mkdirs()
        new File(index2Path).mkdirs()

        new File(index1Path + "part0001.odx").createNewFile()
        new File(index1Path + "part0001.odx.chunks.snappy").createNewFile()
        new File(index1Path + "part0002.odx").createNewFile()
        new File(index1Path + "part0002.odx.chunks.snappy").createNewFile()

        new File(index2Path + "part0001.odx").createNewFile()
        new File(index2Path + "part0001.odx.chunks.snappy").createNewFile()
        new File(index2Path + "part0002.odx").createNewFile()
        new File(index2Path + "part0002.odx.chunks.snappy").createNewFile()

        def indexMeta1 = new ImportMetaIndex(collection, chunkKey, version, "index1", "INDEX1_STRATEGY", "some index source field1")
        ImportHelper.writeIndexProperties(indexMeta1, new File(index1Path + "/index.properties"))
        def indexMeta2 = new ImportMetaIndex(collection, chunkKey, version, "index2", "INDEX2_STRATEGY", "some index source field2")
        ImportHelper.writeIndexProperties(indexMeta2, new File(index2Path + "/index.properties"))
    }

    def "verify loaded data structure"() {
        setup:
        createDataCollectionVersion("testCollection1", "firstChunk", "version1")
        createDataCollectionVersion("testCollection1", "firstChunk", "version2")
        createIndexCollectionVersion("testCollection1", "firstChunk", "version1")
        createIndexCollectionVersion("testCollection1", "firstChunk", "version2")
        writeActiveProperties("testCollection1", "firstChunk", "version2")
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
        indexes[0].path == new File(indexPath.absolutePath + "/testCollection1/firstChunk/version2/index1/")
        indexes[0].strategy == "INDEX1_STRATEGY"
        indexes[1].name == "index2"
        indexes[1].path == new File(indexPath.absolutePath + "/testCollection1/firstChunk/version2/index2/")
        indexes[1].strategy == "INDEX2_STRATEGY"

        when:
        def dataFiles = chunks[0].dataFiles
        then:
        dataFiles.size() == 2
        dataFiles.get("part0001".hashCode()) == new File(dataPath.absolutePath + "/testCollection1/firstChunk/version2/part0001")
        dataFiles.get("part0002".hashCode()) == new File(dataPath.absolutePath + "/testCollection1/firstChunk/version2/part0002")
        cleanup:
        rootPath.delete()
    }
}
