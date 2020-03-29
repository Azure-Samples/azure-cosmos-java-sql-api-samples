# Migration guide

The purpose of this guide is to help easily upgrade to Azure Cosmos DB Java SDK 4.0 for Core (SQL) API ("Java SDK 4.0" from here on out.) The audience for this guide is current users of

* "Legacy" Sync Java SDK 2.x.x
* Async Java SDK 2.x.x
* Java SDK 3.x.x

## Background

| Java SDK                | Release Date | Bundled APIs | Maven Jar                               | API Reference                                             | Release Notes                                                                            |
|-------------------------|--------------|--------------|-----------------------------------------|-----------------------------------------------------------|------------------------------------------------------------------------------------------|
| Async 2.x.x             | June 2018    | Async        | com.microsoft.azure::azure-cosmosdb     | [API](https://azure.github.io/azure-cosmosdb-java/2.0.0/) | [Release Notes](https://docs.microsoft.com/en-us/azure/cosmos-db/sql-api-sdk-async-java) |
| "Legacy" Sync 2.x.x     | Sept 2018    | Sync         | com.microsoft.azure::azure-documentdb   | [API](https://azure.github.io/azure-cosmosdb-java/2.0.0/) | [Release Notes](https://docs.microsoft.com/en-us/azure/cosmos-db/sql-api-sdk-java)       |
| 3.x.x                   | July 2019    | Async/Sync   | com.microsoft.azure::azure-cosmos       | [API](https://azure.github.io/azure-cosmosdb-java/3.0.0/) | -                                                                                        |
| 4.0                     | April 2020   | Async/Sync   | com.azure::azure-cosmos                 | -                                                         | -                                                                                        |

## Breaking API changes

## Code snippet comparisons

### Naming conventions

### Create database

### Create container

### 