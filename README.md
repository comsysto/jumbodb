![jumboDB](http://comsysto.github.com/jumbodb/img/pics/jumbo.png)

Website: http://comsysto.github.com/jumbodb/

Wiki: https://github.com/comsysto/jumbodb/wiki

Quick Installation: https://github.com/comsysto/jumbodb/wiki/Quick-installation-guide

Download: http://repository-comsysto.forge.cloudbees.com/release/org/jumbodb/database/

## What is it good for? 

- As data store for low-latency 'Big Data' apps
- Fast analysis over 'Big Data' with low budget
- Very fast imports (the limitation is the ethernet interface or disk)
- Store, index and query huge amounts of data
- Make your Hadoop outputs accessible to every application (e.g. aggregated statistics)
- Providing billions of datasets in a very short time
- Only immutable data is supported, you cannot insert and update single datasets
- [Data delivery management and versionizing](https://github.com/comsysto/jumbodb/wiki/Data-Delivery-Concept)

## Core ideas of jumboDB

- Process and index the data in a parallelized environment like Hadoop (you can also run it locally)
- All data is immutable, because data usally gets replaced or extended with further data deliveries from Hadoop
- Immutable data allows an easy parallelization in data search
- Preorganized and sorted data is better searchable and results in faster responses
- Sorted data allows grouped read actions
- Sort your data by the major use case to speed up queries
- Compression helps to increase disk speed
- Don't keep all indexes in memory, because the data is too big!

## Features

- Indexing your JSON data 
- Querying over indexed and non-indexed data
- Geospatial indexes
- Range queries (between, greather than, less than and so on)
- Data replication (to another database)
- Sharding and replication (planned, not yet implemented)
- Fast imports
- Multithreaded search
- High compression
- No downtimes on import (data is available until next import is finished)
- Fast rollbacks
- [Data delivery management and versionizing](https://github.com/comsysto/jumbodb/wiki/Data-Delivery-Concept)

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

