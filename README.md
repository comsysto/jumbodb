![jumboDB](http://comsysto.github.com/jumbodb/img/pics/jumbo.png)

Website: http://comsysto.github.com/jumbodb/

Wiki: https://github.com/comsysto/jumbodb/wiki

Quick Installation: https://github.com/comsysto/jumbodb/wiki/Quick-installation-guide

Download: http://repository-comsysto.forge.cloudbees.com/release/org/jumbodb/database/

## What is it good for? 

- data store for low-latency Big Data apps
- allows you to store, index and query huge amounts of data
- provide easy and cheap access to your Hadoop output (e.g. aggregated statistics)
- data versionizing, delivery management
- make your Hadoop outputs accessible to every application in a very short time (Billions of datasets)
- only supports immutable data, you cannot insert and update single datasets

## Core ideas of jumboDB

- process and index the data in a parallized environment like Hadoop (you can also run it locally)
- all data is immutable, because data usally gets replaced or extended with further data deliveries from Hadoop
- immutable data allows a good parallization of data search
- preorganized and sorted data is better searchable, faster results
- sorted data allows grouped read actions
- compression helps to increase disk speed
- don't keep all indexes in memory, because the data is to big!

## Big Data for the masses!

### Balancing performance and cost efficiency

1. Affordable Big Data
Low IO requirements, efficient usage of disk space, low
memory footprint

2. Fast disk access through compression
Snappy achieves compression rates up to 5 times
increasing disk IO efficiency and saving storage cost

3. Batch processing - delivery driven approach
"Write once - read many" one batch of data is an atomic
write with the rollback possibility

4. Supports JSON documents
Schema flexibility for rapid application development

5. Power and scalability of Apache Hadoop
For batch processing, aggregation and indexing of your
data.(e.g. writes up to 500.000 JSON documents per second into the data store)

6. Low read latency for end-user apps
Optimized querying even for large result sets through
multithreading and efficient data streaming (e.g. 100.000
JSON documents returned in less than a second)

7. Hadoop Connector, Java Driver and R connector are available

## Setup JumboDB

Please see the JumboDB Wiki https://github.com/comsysto/jumbodb/wiki

## Licenses

The connectors are licensed under Apache License 2.0: http://www.apache.org/licenses/LICENSE-2.0.html

The database is licensed under Apache License 2.0: http://www.apache.org/licenses/LICENSE-2.0.html

