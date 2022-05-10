// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.examples.changefeed;

import com.azure.cosmos.ChangeFeedProcessor;
import com.azure.cosmos.ChangeFeedProcessorBuilder;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.examples.common.CustomPOJO2;
import com.azure.cosmos.implementation.Utils;
import com.azure.cosmos.models.ChangeFeedProcessorState;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.azure.cosmos.examples.changefeed.SampleChangeFeedProcessor.createNewCollection;
import static com.azure.cosmos.examples.changefeed.SampleChangeFeedProcessor.createNewDatabase;
import static com.azure.cosmos.examples.changefeed.SampleChangeFeedProcessor.createNewDocumentsCustomPOJO;
import static com.azure.cosmos.examples.changefeed.SampleChangeFeedProcessor.createNewLeaseCollection;
import static com.azure.cosmos.examples.changefeed.SampleChangeFeedProcessor.deleteDatabase;
import static com.azure.cosmos.examples.changefeed.SampleChangeFeedProcessor.getCosmosClient;

/**
 * Sample for Change Feed Estimator.
 * This sample models an application where documents are being inserted into one container (the "feed container"),
 * and meanwhile another worker thread or worker application is pulling inserted documents from the feed container's Change Feed
 * and operating on them in some way. For one or more workers to process the Change Feed of a container, the workers must first contact the server
 * and "lease" access to monitor one or more partitions of the feed container. The Change Feed Processor Library
 * handles leasing automatically for you, however you must create a separate "lease container" where the Change Feed
 * Processor Library can store and track leases container partitions.
 */
public class SampleChangeFeedEstimator {

    public static final String DATABASE_NAME = "db_" + UUID.randomUUID();
    public static final String COLLECTION_NAME = "coll_" + UUID.randomUUID();
    private static final ObjectMapper OBJECT_MAPPER = Utils.getSimpleObjectMapper();
    protected static Logger logger = LoggerFactory.getLogger(SampleChangeFeedEstimator.class);

    public static void main(String[] args) {
        logger.info("Begin Sample");
        try {
            //Summary of the next four commands:
            //-Create an asynchronous Azure Cosmos DB client and database so that we can issue async requests to the DB
            //-Create a "feed container" and a "lease container" in the DB
            logger.info("Create CosmosClient");
            CosmosAsyncClient client = getCosmosClient();

            logger.info("Create sample's database: " + DATABASE_NAME);
            CosmosAsyncDatabase cosmosDatabase = createNewDatabase(client, DATABASE_NAME);

            logger.info("Create container for documents: " + COLLECTION_NAME);
            CosmosAsyncContainer feedContainer = createNewCollection(client, DATABASE_NAME, COLLECTION_NAME);

            logger.info("Create container for lease: " + COLLECTION_NAME + "-leases");
            CosmosAsyncContainer leaseContainer = createNewLeaseCollection(client, DATABASE_NAME, COLLECTION_NAME + "-leases");

            //Model of a worker thread or application which leases access to monitor one or more feed container
            //partitions via the Change Feed. In a real-world application you might deploy this code in an Azure function.
            //The next line causes the worker to create and start an instance of the Change Feed Processor. See the implementation of getChangeFeedProcessor() for guidance
            //on creating a handler for Change Feed events. In this stream, we also trigger the insertion of 10 documents on a separate
            //thread.
            logger.info("Start Change Feed Processor on worker (handles changes asynchronously)");
            ChangeFeedProcessor changeFeedProcessorInstance = new ChangeFeedProcessorBuilder()
                .hostName("SampleHost_1")
                .feedContainer(feedContainer)
                .leaseContainer(leaseContainer)
                .handleChanges(handleChangesWithLag())
                .buildChangeFeedProcessor();

            changeFeedProcessorInstance.start()
                                       .subscribeOn(Schedulers.boundedElastic())
                                       .subscribe();

            //These two lines model an application which is inserting ten documents into the feed container
            logger.info("Start application that inserts documents into feed container");
            createNewDocumentsCustomPOJO(feedContainer, 10, Duration.ofSeconds(3));

            Thread.sleep(Duration.ofSeconds(4).toMillis());

            AtomicInteger totalLag = new AtomicInteger();

            // <EstimatedLag>
            Mono<Map<String, Integer>> estimatedLagResult = changeFeedProcessorInstance
                .getEstimatedLag()
                .map(estimatedLag -> {
                    try {
                        logger.info("Estimated lag result is : {}", OBJECT_MAPPER.writeValueAsString(estimatedLag));
                     } catch (JsonProcessingException ex) {
                         logger.error("Unexpected", ex);
                     }
                     return estimatedLag;
                })
                .map(estimatedLag -> {
                    for (int lag : estimatedLag.values()) {
                        totalLag.addAndGet(lag);
                    }
                    logger.info("Total lag is : {}", totalLag.get());
                    return estimatedLag;
                });

            estimatedLagResult.subscribe();
            // </EstimatedLag>

            // <EstimatedLagWithState>
            Mono<List<ChangeFeedProcessorState>> currentState = changeFeedProcessorInstance.getCurrentState();
            currentState.map(state -> {
                for (ChangeFeedProcessorState changeFeedProcessorState : state) {
                    int estimatedLag = changeFeedProcessorState.getEstimatedLag();
                    String hostName = changeFeedProcessorState.getHostName();
                    logger.info("Host name {} with estimated lag {}", hostName, estimatedLag);
                }
                return state;
            }).subscribe();
            // </EstimatedLagWithState>

            //When all documents have been processed, clean up
            changeFeedProcessorInstance.stop().subscribe();

            logger.info("Delete sample's database: " + DATABASE_NAME);
            deleteDatabase(cosmosDatabase);

            Thread.sleep(500);
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.info("End Sample");
    }

    private static Consumer<List<JsonNode>> handleChangesWithLag() {
        return (List<JsonNode> docs) -> {
            logger.info("Start handleChangesWithLag()");

            try {
                Thread.sleep(Duration.ofSeconds(5).toMillis());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            for (JsonNode document : docs) {
                try {
                    //Change Feed hands the document to you in the form of a JsonNode
                    //As a developer you have two options for handling the JsonNode document provided to you by Change Feed
                    //One option is to operate on the document in the form of a JsonNode, as shown below. This is great
                    //especially if you do not have a single uniform data model for all documents.
                    logger.info("Document received: " + OBJECT_MAPPER.writerWithDefaultPrettyPrinter()
                                                                     .writeValueAsString(document));

                    //You can also transform the JsonNode to a POJO having the same structure as the JsonNode,
                    //as shown below. Then you can operate on the POJO.
                    CustomPOJO2 pojo_doc = OBJECT_MAPPER.treeToValue(document, CustomPOJO2.class);
                    logger.info("id: " + pojo_doc.getId());

                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
            logger.info("End handleChangesWithLag()");
        };
    }
}
