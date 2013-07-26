package org.jumbodb.benchmark.generator

import org.jumbodb.benchmark.generator.job.DataDeliveryGenerator
import spock.lang.Specification


/**
 * @author Ulf Gitschthaler
 */
class DataDeliveryGeneratorSpec extends Specification {


    def "data generator stops on missing config file"(){
        when:
        DataDeliveryGenerator.main()
        then:
        thrown(IllegalArgumentException.class)
    }

    // ULF fix
    def "data generator starts with valid config file"(){
//        given:
//        def fakeConfigFile = File.createTempFile("startup", "");
//        def dataGenerator = new DataDeliveryGenerator()
//        dataGenerator.dataGenerator = Mock(DataDeliveryGenerator.class)
//        when:
//        dataGenerator.main(fakeConfigFile.getAbsolutePath())
//        then:
//        1 * dataGenerator.run(_)

    }

    def "data generator parses valid config file"() {
        given:
        def configFile = this.class.getResource("/testConfigFile.json")
        when:
        def config = new DataDeliveryGenerator().parseConfigFile(new File(configFile.path))
        then:
        config.description == "Sample data with 5GB and 3 collections"
        config.collections.size() == 3
        config.collections[0].dataSetSizeInChars == 1000
    }

}