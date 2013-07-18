package org.jumbodb.benchmark.generator


import spock.lang.Specification


/**
 * @author Ulf Gitschthaler
 */
class DataGeneratorSpec extends Specification {


    def "data generator stops on missing config file"(){
        when:
        DataGenerator.main()
        then:
        thrown(IllegalArgumentException.class)
    }

    def "data generator starts with valid config file"(){
//        given:
//        def fakeConfigFile = File.createTempFile("startup", "");
//        def dataGenerator = new DataGenerator()
//        dataGenerator.dataGenerator = Mock(DataGenerator.class)
//        when:
//        dataGenerator.main(fakeConfigFile.getAbsolutePath())
//        then:
//        1 * dataGenerator.run(_)

    }

    def "data generator parses valid config file"() {
        given:
        def configFile = this.class.getResource("/testConfigFile.json")
        when:
        def config = new DataGenerator().parseConfigFile(new File(configFile.path))
        then:
        config.description == "Sample data with 5GB and 3 collections"
        config.collections.size() == 3
        config.collections[0].dataSetSizeInChars == 1000
    }

}