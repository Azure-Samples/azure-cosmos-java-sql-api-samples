# Migration guide

The purpose of this guide is to help easily upgrade to Azure Cosmos DB Java SDK 4.0 for Core (SQL) API ("Java SDK 4.0" from here on out.) The audience for this guide is current users of

* "Legacy" Sync Java SDK 2.x.x
* Async Java SDK 2.x.x
* Java SDK 3.x.x

## Background

| Java SDK                | Release Date | Bundled APIs         | Maven Jar                               | Java package name |API Reference                                             | Release Notes                                                                            |
|-------------------------|--------------|----------------------|-----------------------------------------|-------------------|-----------------------------------------------------------|------------------------------------------------------------------------------------------|
| Async 2.x.x             | June 2018    | Async(RxJava)        | com.microsoft.azure::azure-cosmosdb     |                   | [API](https://azure.github.io/azure-cosmosdb-java/2.0.0/) | [Release Notes](https://docs.microsoft.com/en-us/azure/cosmos-db/sql-api-sdk-async-java) |
| "Legacy" Sync 2.x.x     | Sept 2018    | Sync                 | com.microsoft.azure::azure-documentdb   |                   | [API](https://azure.github.io/azure-cosmosdb-java/2.0.0/) | [Release Notes](https://docs.microsoft.com/en-us/azure/cosmos-db/sql-api-sdk-java)       |
| 3.x.x                   | July 2019    | Async(Reactor)/Sync  | com.microsoft.azure::azure-cosmos       |                   | [API](https://azure.github.io/azure-cosmosdb-java/3.0.0/) | -                                                                                        |
| 4.0                     | April 2020   | Async(Reactor)/Sync  | com.azure::azure-cosmos                 |                   | -                                                         | -                                                                                        |

## Important implementation changes

### RxJava replaced with reactor in Java SDK 3.x.x and 4.0

If you have been using a pre-3.x.x Java SDK, it is recommended to review our [Reactor pattern guide](reactor-pattern-guide.md) for an introduction to async programming and Reactor.

Users of the Async Java SDK 2.x.x will want to review our [Reactor vs RxJava Guide]() for additional guidance on converting RxJava code to use Reactor.

## Important API changes

### Naming conventions 

![Java SDK naming conventions](media/java_sdk_naming_conventions.jpg)

* Java SDK 3.x.x and 4.0 refer to clients, resources, etc. as ```Cosmos```*X*; for example ```CosmosClient```, ```CosmosDatabase```, ```CosmosContainer```..., whereas version 2.x.x Java SDKs did not have a uniform naming scheme.

* Java SDK 3.x.x and 4.0 offer Sync and Async APIs. 
    * **Java SDK 4.0**: classes belong to the Sync API unless the name has ```Async``` after ```Cosmos```. 
    * **Java SDK 3.x.x**: classes belong to the Async API unless the name has ```Sync``` after Cosmos.
    * **Async Java SDK 2.x.x**: similar class names to **Sync Java SDK 2.x.x** but the class name starts with ```Async```.

### Representing items

### Imports

### Accessors

### QueryMetrics


## Code snippet comparisons

### Create resources

### Item operations

### Indexing

### Stored procedures

### Change Feed