// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.examples.changefeed;

import com.azure.cosmos.ChangeFeedProcessor;
import com.azure.cosmos.ChangeFeedProcessorBuilder;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.examples.common.CustomPOJO2;
import com.azure.cosmos.implementation.Utils;
import com.azure.cosmos.implementation.apachecommons.lang.RandomStringUtils;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerRequestOptions;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.ThroughputProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;

/**
 * Sample for Change Feed Processor.
 * This sample models an application where documents are being inserted into one container (the "feed container"),
 * and meanwhile another worker thread or worker application is pulling inserted documents from the feed container's Change Feed
 * and operating on them in some way. For one or more workers to process the Change Feed of a container, the workers must first contact the server
 * and "lease" access to monitor one or more partitions of the feed container. The Change Feed Processor Library
 * handles leasing automatically for you, however you must create a separate "lease container" where the Change Feed
 * Processor Library can store and track leases container partitions.
 */
public class SampleChangeFeedProcessor {

    public static int WAIT_FOR_WORK = 60000;
    public static final String DATABASE_NAME = "db_" + RandomStringUtils.randomAlphabetic(7);
    public static final String COLLECTION_NAME = "coll_" + RandomStringUtils.randomAlphabetic(7);
    private static final ObjectMapper OBJECT_MAPPER = Utils.getSimpleObjectMapper();
    protected static Logger logger = LoggerFactory.getLogger(SampleChangeFeedProcessor.class);


    private static ChangeFeedProcessor changeFeedProcessorInstance;
    private static boolean isWorkCompleted = false;

    public static void main(String[] args) {
        logger.info("BEGIN Sample");

        try {

            //Summary of the next four commands:
            //-Create an asynchronous Azure Cosmos DB client and database so that we can issue async requests to the DB
            //-Create a "feed container" and a "lease container" in the DB
            logger.info("-->CREATE DocumentClient");
            CosmosAsyncClient client = getCosmosClient();

            logger.info("-->CREATE sample's database: " + DATABASE_NAME);
            CosmosAsyncDatabase cosmosDatabase = createNewDatabase(client, DATABASE_NAME);

            logger.info("-->CREATE container for documents: " + COLLECTION_NAME);
            CosmosAsyncContainer feedContainer = createNewCollection(client, DATABASE_NAME, COLLECTION_NAME);

            logger.info("-->CREATE container for lease: " + COLLECTION_NAME + "-leases");
            CosmosAsyncContainer leaseContainer = createNewLeaseCollection(client, DATABASE_NAME, COLLECTION_NAME + "-leases");

            //Model of a worker thread or application which leases access to monitor one or more feed container
            //partitions via the Change Feed. In a real-world application you might deploy this code in an Azure function.
            //The next line causes the worker to create and start an instance of the Change Feed Processor. See the implementation of getChangeFeedProcessor() for guidance
            //on creating a handler for Change Feed events. In this stream, we also trigger the insertion of 10 documents on a separate
            //thread.
            logger.info("-->START Change Feed Processor on worker (handles changes asynchronously)");
            changeFeedProcessorInstance = getChangeFeedProcessor("SampleHost_1", feedContainer, leaseContainer);
            changeFeedProcessorInstance.start()
                    .subscribeOn(Schedulers.elastic())
                    .doOnSuccess(aVoid -> {
                        //pass
                    })
                    .subscribe();

            //These two lines model an application which is inserting ten documents into the feed container
            logger.info("-->START application that inserts documents into feed container");
            createNewDocumentsCustomPOJO(feedContainer, 10, Duration.ofSeconds(3));
            isWorkCompleted = true;

            //This loop models the Worker main loop, which spins while its Change Feed Processor instance asynchronously
            //handles incoming Change Feed events from the feed container. Of course in this sample, polling
            //isWorkCompleted is unnecessary because items are being added to the feed container on the same thread, and you
            //can see just above isWorkCompleted is set to true.
            //But conceptually the worker is part of a different thread or application than the one which is inserting
            //into the feed container; so this code illustrates the worker waiting and listening for changes to the feed container
            long remainingWork = WAIT_FOR_WORK;
            while (!isWorkCompleted && remainingWork > 0) {
                Thread.sleep(100);
                remainingWork -= 100;
            }

            //When all documents have been processed, clean up
            if (isWorkCompleted) {
                if (changeFeedProcessorInstance != null) {
                    changeFeedProcessorInstance.stop().subscribe();
                }
            } else {
                throw new RuntimeException("The change feed processor initialization and automatic create document feeding process did not complete in the expected time");
            }

            logger.info("-->DELETE sample's database: " + DATABASE_NAME);
            deleteDatabase(cosmosDatabase);

            Thread.sleep(500);

        } catch (Exception e) {
            e.printStackTrace();
        }

        logger.info("END Sample");
    }

    public static ChangeFeedProcessor getChangeFeedProcessor(String hostName, CosmosAsyncContainer feedContainer, CosmosAsyncContainer leaseContainer) {
        return new ChangeFeedProcessorBuilder()
                .hostName(hostName)
                .feedContainer(feedContainer)
                .leaseContainer(leaseContainer)
                .handleChanges((List<JsonNode> docs) -> {
                    logger.info("--->setHandleChanges() START");

                    for (JsonNode document : docs) {
                        try {
                            //Change Feed hands the document to you in the form of a JsonNode
                            //As a developer you have two options for handling the JsonNode document provided to you by Change Feed
                            //One option is to operate on the document in the form of a JsonNode, as shown below. This is great
                            //especially if you do not have a single uniform data model for all documents.
                            logger.info("---->DOCUMENT RECEIVED: " + OBJECT_MAPPER.writerWithDefaultPrettyPrinter()
                                    .writeValueAsString(document));

                            //You can also transform the JsonNode to a POJO having the same structure as the JsonNode,
                            //as shown below. Then you can operate on the POJO.
                            CustomPOJO2 pojo_doc = OBJECT_MAPPER.treeToValue(document, CustomPOJO2.class);
                            logger.info("----=>id: " + pojo_doc.getId());

                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    }
                    logger.info("--->handleChanges() END");

                })
                .buildChangeFeedProcessor();
    }

    public static CosmosAsyncClient getCosmosClient() {

        return new CosmosClientBuilder()
                .endpoint(SampleConfigurations.HOST)
                .key(SampleConfigurations.MASTER_KEY)
                .contentResponseOnWriteEnabled(true)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .buildAsyncClient();
    }

    public static CosmosAsyncDatabase createNewDatabase(CosmosAsyncClient client, String databaseName) {
        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName).block();
        return client.getDatabase(databaseResponse.getProperties().getId());
    }

    public static void deleteDatabase(CosmosAsyncDatabase cosmosDatabase) {
        cosmosDatabase.delete().block();
    }

    public static CosmosAsyncContainer createNewCollection(CosmosAsyncClient client, String databaseName, String collectionName) {
        CosmosAsyncDatabase databaseLink = client.getDatabase(databaseName);
        CosmosAsyncContainer collectionLink = databaseLink.getContainer(collectionName);
        CosmosContainerResponse containerResponse = null;

        try {
            containerResponse = collectionLink.read().block();

            if (containerResponse != null) {
                throw new IllegalArgumentException(String.format("Collection %s already exists in database %s.", collectionName, databaseName));
            }
        } catch (RuntimeException ex) {
            if (ex instanceof CosmosException) {
                CosmosException CosmosException = (CosmosException) ex;

                if (CosmosException.getStatusCode() != 404) {
                    throw ex;
                }
            } else {
                throw ex;
            }
        }

        CosmosContainerProperties containerSettings = new CosmosContainerProperties(collectionName, "/pk");
        CosmosContainerRequestOptions requestOptions = new CosmosContainerRequestOptions();

        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(10000);

        containerResponse = databaseLink.createContainer(containerSettings, throughputProperties, requestOptions).block();

        if (containerResponse == null) {
            throw new RuntimeException(String.format("Failed to create collection %s in database %s.", collectionName, databaseName));
        }

        return databaseLink.getContainer(containerResponse.getProperties().getId());
    }

    public static CosmosAsyncContainer createNewLeaseCollection(CosmosAsyncClient client, String databaseName, String leaseCollectionName) {
        CosmosAsyncDatabase databaseLink = client.getDatabase(databaseName);
        CosmosAsyncContainer leaseCollectionLink = databaseLink.getContainer(leaseCollectionName);
        CosmosContainerResponse leaseContainerResponse = null;

        try {
            leaseContainerResponse = leaseCollectionLink.read().block();

            if (leaseContainerResponse != null) {
                leaseCollectionLink.delete().block();

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        } catch (RuntimeException ex) {
            if (ex instanceof CosmosException) {
                CosmosException CosmosException = (CosmosException) ex;

                if (CosmosException.getStatusCode() != 404) {
                    throw ex;
                }
            } else {
                throw ex;
            }
        }

        CosmosContainerProperties containerSettings = new CosmosContainerProperties(leaseCollectionName, "/id");
        CosmosContainerRequestOptions requestOptions = new CosmosContainerRequestOptions();

        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);

        leaseContainerResponse = databaseLink.createContainer(containerSettings, throughputProperties, requestOptions).block();

        if (leaseContainerResponse == null) {
            throw new RuntimeException(String.format("Failed to create collection %s in database %s.", leaseCollectionName, databaseName));
        }

        return databaseLink.getContainer(leaseContainerResponse.getProperties().getId());
    }

    public static void createNewDocumentsCustomPOJO(CosmosAsyncContainer containerClient, int count, Duration delay) {
        String suffix = RandomStringUtils.randomAlphabetic(10);
        for (int i = 0; i <= count; i++) {
            CustomPOJO2 document = new CustomPOJO2();
            document.setId(String.format("0%d-%s", i, suffix));
            document.setPk(document.getId()); // This is a very simple example, so we'll just have a partition key (/pk) field that we set equal to id

            containerClient.createItem(document).subscribe(doc -> {
                logger.info("---->DOCUMENT WRITE: " + doc);
            });

            long remainingWork = delay.toMillis();
            try {
                while (remainingWork > 0) {
                    Thread.sleep(100);
                    remainingWork -= 100;
                }
            } catch (InterruptedException iex) {
                // exception caught
                break;
            }
        }
    }
}
