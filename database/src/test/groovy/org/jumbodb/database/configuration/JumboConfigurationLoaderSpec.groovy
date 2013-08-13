package org.jumbodb.database.configuration

import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class JumboConfigurationLoaderSpec extends Specification {

    def "load default configuration"() {
        setup:
        def loader = new JumboConfigurationLoader(new File("does/not/exist"))
        when:
        def configuration = loader.loadConfiguration()
        then:
        configuration.getProperty("jumbodb.import.port") == "12001"
        configuration.getProperty("jumbodb.query.port") == "12002"
        configuration.getProperty("jumbodb.datapath") == (System.getProperty("user.home") + "/jumbodb/data")
        configuration.getProperty("jumbodb.indexpath") == (System.getProperty("user.home") + "/jumbodb/index")
        configuration.getProperty("jumbodb.index.threadpool.size") == "30"
        configuration.getProperty("jumbodb.data.threadpool.size") == "20"
    }

    def "load system configuration configuration"() {
        setup:
        def systemFile = File.createTempFile("config", "jumbo")
        Properties p = new Properties()
        p.setProperty("jumbodb.import.port", "13001")
        p.setProperty("jumbodb.query.port", "13002")
        p.setProperty("jumbodb.datapath", "blub_path")
        p.setProperty("jumbodb.indexpath", "blob_path")
        def fos = new FileOutputStream(systemFile)
        p.store(fos, "test file")
        fos.close()
        def loader = new JumboConfigurationLoader(systemFile)
        when:
        def configuration = loader.loadConfiguration()
        then:
        configuration.getProperty("jumbodb.import.port") == "13001"
        configuration.getProperty("jumbodb.query.port") == "13002"
        configuration.getProperty("jumbodb.datapath") == "blub_path"
        configuration.getProperty("jumbodb.indexpath") == "blob_path"
        configuration.getProperty("jumbodb.index.threadpool.size") == "30"  // loaded from default
        configuration.getProperty("jumbodb.data.threadpool.size") == "20" // loaded from default
        cleanup:
        systemFile.delete()
    }

    def "load configuration from specified file"() {
        setup:
        def systemFile = File.createTempFile("config", "jumbo")
        Properties p = new Properties()
        p.setProperty("jumbodb.import.port", "13001")
        p.setProperty("jumbodb.query.port", "13002")
        p.setProperty("jumbodb.datapath", "blub_path")
        p.setProperty("jumbodb.indexpath", "blob_path")
        def fos = new FileOutputStream(systemFile)
        p.store(fos, "test file")
        fos.close()
        System.setProperty("jumbodb.config", systemFile.getAbsolutePath())
        def loader = new JumboConfigurationLoader(new File("does/not/exist"))
        when:
        def configuration = loader.loadConfiguration()
        then:
        configuration.getProperty("jumbodb.import.port") == "13001"
        configuration.getProperty("jumbodb.query.port") == "13002"
        configuration.getProperty("jumbodb.datapath") == "blub_path"
        configuration.getProperty("jumbodb.indexpath") == "blob_path"
        configuration.getProperty("jumbodb.index.threadpool.size") == "30"  // loaded from default
        configuration.getProperty("jumbodb.data.threadpool.size") == "20" // loaded from default
        cleanup:
        systemFile.delete()
    }
}
