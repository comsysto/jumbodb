jumboDB
=======

http://comsysto.github.com/jumbodb/

## Data Store for low-latency Big Data Apps

Already using Apache Hadoop for batch data deliveries? Need
the efficient store for your analytical application?

jumboDB allows you to store, index and query large amounts of
data using Apache Hadoop core features

## Big Data for the masses!

### Balancing performance and cost efficiency

1. Affordable Big Data
Low IO requirements, efficient usage of disk space, low
memory footprint

2. Fast disk access through compression
Snappy achieves compression rates up to 10 times
increasing disk IO efficiency and saving storage cost

3. Batch processing - delivery driven approach
"Write once - read many" one batch of data is an atomic
write with the rollback possibility

4. Supports JSON documents
Schema flexibility for rapid application development

5. Power and scalability of Apache Hadoop
For batch processing, aggregation and indexing of your
data.(e.g. writes up to 500.000 JSON documents per second)

6. Low read latency for end-user apps
Optimized querying even for large result sets through
multithreading and efficient data streaming (e.g. 100.000
JSON documents returned in less than a second)

7. Hadoop Connector and Java Driver available

## How to setup

### Requirements

* Java 1.6 or higher
* Gradle 1.4
    * Download & install (must be in $PATH) http://www.gradle.org
    * Mac user with brew:  `brew install gradle`
* Play Framework 2.1
    * Download & install (must be in $PATH) http://www.playframework.com/

### Setup project

* Eclipse users do `gradle eclipse` in the root folder
* Intellij IDEA users do `gradle idea`

### Project structure

* `connectors` contains all the connectors like java and hadoop connector
* `database` contains the database
* `test` contains different things, like integration tests, evaluations and test data generation

### How to run the database

Run the database
`cd database`
`play start`

Data is stored by default in your home folder under `jumbodb`.

### Use the connectors

_Gradle_

Repositories
```
repositories {
    mavenCentral()
    mavenRepo(url: "http://repository-comsysto.forge.cloudbees.com/release")
    mavenRepo(url: "http://repository-comsysto.forge.cloudbees.com/snapshot")
}
```

Dependency
```
compile "org.jumbodb.connector:jumbodb-java-connector:0.0.3"
```

_Maven_

Repository
```
<repositories>
    <repository>
      <id>comsysto-release</id>
      <url>http://repository-comsysto.forge.cloudbees.com/release</url>
    </repository>
    <repository>
      <id>comsysto-snapshot</id>
      <url>http://repository-comsysto.forge.cloudbees.com/snapshot</url>
    </repository>
  </repositories>
```

Dependency
```
<dependency>
  <groupId>org.jumbodb.connector::</groupId>
  <artifactId>jumbodb-java-connector</artifactId>
  <version>0.0.3</version>
  <type>jar</type>
  <scope>compile</scope>
</dependency>
```

### Release the connectors

Create `~/.gradle/gradle.properties` in your home folder with the following contents:

```
cloudbeesUsername=your user name
cloudbeesPassword=your password
cloudbeesAccountName=comsysto`
```

Set the current version in the root `build.gradle`
`cd connectors`
`gradle uploadArchives`

### Release the database

`cd database`
`play dist` produces a full bundled distribution. Just unzip and call ./start






