package org.jumbodb.common.util.config

import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll


/**
 * @author Ulf Gitschthaler
 */
class JSONConfigReaderSpec extends Specification {

    @Shared
    def userHome = System.getProperty("user.home")


    @Ignore("does not work on windows") // TODO does not work on windows
    def "should parse config file with possible replacement"(){
        expect:
        def config = JSONConfigReader.read(FakeConfig.class, file.absolutePath) as FakeConfig
        config.foo==foo
        config.configFile==configFile
        config.user==user
        where:
        file                    | foo   | configFile                            | user
        createMatchingFile()    | "bar" | userHome + "/.jumbodb/generated_data" | "sample"
        createNonMatchingFile() | "bar" | '/var/tmp/.jumbodb/generated_data'    | "sample"
    }

    @Unroll("should replace #placeholer with #replacement")
    def "should replace username"(){
        setup:
        def prefix = "{\"foo\": \"bar\", \"configFile\": "
        def suffix = "/.jumbodb/generated_data\", \"user\": \"sample\" }"
        expect:
        def input = prefix + placeholer + suffix
        def output = prefix + replacement + suffix
        output == JSONConfigReader.replaceUserHome(input)
        where:
        placeholer      | replacement
        '$USER_HOME'    | userHome
        '$user_home'    | '$user_home'
        '%USER_HOME%'   | userHome
        '%user_home%'   | '%user_home%'
    }

    def File createMatchingFile(){
        def content = '{"foo": "bar", "configFile": "%USER_HOME%/.jumbodb/generated_data", "user": "sample" }'
        def file = File.createTempFile("test", ".json")
        file.write(content)
        return file
    }

    def File createNonMatchingFile(){
        def content = '{"foo": "bar", "configFile": "/var/tmp/.jumbodb/generated_data", "user": "sample" }'
        def file = File.createTempFile("test", ".json")
        file.write(content)
        return file
    }
}