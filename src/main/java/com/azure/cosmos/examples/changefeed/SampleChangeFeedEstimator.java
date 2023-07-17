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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.azure.cosmos.examples.changefeed.SampleChangeFeedProcessor.createNewCollection;
import static com.azure.cosmos.examples.changefeed.SampleChangeFeedProcessor.createNewDatabase;
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

            // <ChangeFeedProcessorBuilder>
            ChangeFeedProcessor changeFeedProcessorMainInstance = new ChangeFeedProcessorBuilder()
                .hostName("SampleHost_1")
                .feedContainer(feedContainer)
                .leaseContainer(leaseContainer)
                .handleChanges(handleChangesWithLag())
                .buildChangeFeedProcessor();

            try {
                changeFeedProcessorMainInstance
                    .start()
                    .subscribeOn(Schedulers.boundedElastic())
                    .timeout(Duration.ofSeconds(10))
                    .then(Mono.just(changeFeedProcessorMainInstance)
                              .delayElement(Duration.ofSeconds(10))
                              .flatMap(value -> changeFeedProcessorMainInstance
                                  .stop()
                                  .subscribeOn(Schedulers.boundedElastic())
                                  .timeout(Duration.ofSeconds(10))))
                    .subscribe();
            } catch (Exception ex) {
                logger.error("Change feed processor did not start and stopped in the expected time", ex);
                throw ex;
            }
            // </ChangeFeedProcessorBuilder>

            //  Sleeping here because the CFP instance above requires some time to initialize the lease container
            //  with all the documents that will keep track of the ongoing work.
            Thread.sleep(Duration.ofSeconds(20).toMillis());

            //  CFP lag checking can be performed on a separate application (like a health monitor)
            //  as long as the same input containers (feedContainer and leaseContainer) and the exact same lease prefix are used.
            //  The estimator code requires that the CFP had an opportunity to fully initialize the leaseContainer's documents.
            ChangeFeedProcessor changeFeedProcessorSideCart = new ChangeFeedProcessorBuilder()
                .hostName("side-cart")
                .feedContainer(feedContainer)
                .leaseContainer(leaseContainer)
                .handleChanges(nodes -> {
                    logger.error("This never needs to be called, as this is just monitoring the state");
                })
                .buildChangeFeedProcessor();

            AtomicInteger totalLag = new AtomicInteger();
            // <EstimatedLag>
            Mono<List<ChangeFeedProcessorState>> currentState = changeFeedProcessorMainInstance.getCurrentState();
            currentState.map(state -> {
                for (ChangeFeedProcessorState changeFeedProcessorState : state) {
                    totalLag.addAndGet(changeFeedProcessorState.getEstimatedLag());
                }
                return state;
            }).subscribe();

            // Initially totalLag should be zero
            logger.info("Initially total lag is : {}", totalLag.get());

            totalLag.set(0);
            currentState = changeFeedProcessorSideCart.getCurrentState();
            currentState.map(state -> {
                for (ChangeFeedProcessorState changeFeedProcessorState : state) {
                    totalLag.addAndGet(changeFeedProcessorState.getEstimatedLag());
                }
                return state;
            }).subscribe();

            // Initially totalLag should be zero
            logger.info("Initially total lag is : {}", totalLag.get());
            // </EstimatedLag>

            //These two lines model an application which is inserting ten documents into the feed container
            logger.info("Start application that inserts documents into feed container");
            createNewDocumentsCustomPOJO(feedContainer, 10);

            currentState = changeFeedProcessorMainInstance.getCurrentState();
            currentState.map(state -> {
                for (ChangeFeedProcessorState changeFeedProcessorState : state) {
                    totalLag.addAndGet(changeFeedProcessorState.getEstimatedLag());
                }
                return state;
            }).block();

            // Finally, totalLag should be greater or equal to the number of documents created
            logger.info("Finally total lag is : {}", totalLag.get());

            // <FinalLag>
            totalLag.set(0);
            changeFeedProcessorSideCart.start().block();
            currentState = changeFeedProcessorSideCart.getCurrentState();
            currentState.map(state -> {
                for (ChangeFeedProcessorState changeFeedProcessorState : state) {
                    totalLag.addAndGet(changeFeedProcessorState.getEstimatedLag());
                }
                return state;
            }).block();

            // Finally, totalLag should be greater or equal to the number of documents created
            logger.info("Finally total lag is : {}", totalLag.get());
            // </FinalLag>

            Thread.sleep(Duration.ofSeconds(30).toMillis());

            logger.info("Delete sample's database: " + DATABASE_NAME);
            deleteDatabase(cosmosDatabase);

            Thread.sleep(500);
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.info("End Sample");
    }

    // <HandleChangesWithLag>
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
    // </HandleChangesWithLag>

    public static void createNewDocumentsCustomPOJO(CosmosAsyncContainer containerClient, int count) {
        String suffix = UUID.randomUUID().toString();
        for (int i = 0; i < count; i++) {
            CustomPOJO2 document = new CustomPOJO2();
            document.setId(String.format("0%d-%s", i, suffix));
            document.setPk(document.getId()); // This is a very simple example, so we'll just have a partition key (/pk) field that we set equal to id

            containerClient.createItem(document).block();
        }
    }
}
