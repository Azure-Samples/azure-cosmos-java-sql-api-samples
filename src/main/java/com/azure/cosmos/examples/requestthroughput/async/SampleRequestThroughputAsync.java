package com.azure.cosmos.examples.requestthroughput.async;

import com.azure.cosmos.ConnectionPolicy;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosClientException;
import com.azure.cosmos.examples.changefeed.SampleChangeFeedProcessor;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.implementation.Utils;
import com.azure.cosmos.models.CosmosAsyncItemResponse;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.PartitionKey;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class SampleRequestThroughputAsync {

    private static CosmosAsyncClient client;
    private static CosmosAsyncDatabase database;
    private static CosmosAsyncContainer container;
    protected static Logger logger = LoggerFactory.getLogger(SampleChangeFeedProcessor.class.getSimpleName());

    public static void main(String[] args) {
        try {
            requestThroughputDemo();
        } catch(Exception err) {
            logger.error("Failed running demo: ", err);
        }
    }

    public static void requestThroughputDemo() {
        client = new CosmosClientBuilder()
                .setEndpoint(AccountSettings.HOST)
                .setKey(AccountSettings.MASTER_KEY)
                .setConnectionPolicy(ConnectionPolicy.getDefaultPolicy())
                .setConsistencyLevel(ConsistencyLevel.EVENTUAL)
                .buildAsyncClient();

        AtomicBoolean resourcesCreated = new AtomicBoolean();
        resourcesCreated.set(false);

        // This code describes the logic of database and container creation as a reactive stream (but crucially doesn't actually create anything).
        Mono<Void> databaseContainerIfNotExist = client.createDatabaseIfNotExists("ContosoInventoryDB").flatMap(databaseResponse -> {
            database = databaseResponse.getDatabase();
            logger.info("Got DB.");
            CosmosContainerProperties containerProperties = new CosmosContainerProperties("ContosoInventoryContainer", "/id");
            return database.createContainerIfNotExists(containerProperties, 400);
        }).flatMap(containerResponse -> {
            container = containerResponse.getContainer();
            logger.info("Got container.");
            return Mono.empty();
        });

        // This code asynchronously sends a request to create a database and container (returns immediately).
        // When the server sends a response back, a flag is set to notify the rest of the program.
        logger.info("Creating database and container asynchronously...");
        databaseContainerIfNotExist.subscribe(voidItem -> {}, err -> {},
                () -> {
                    logger.info("Finished creating resources.\n\n");
                    resourcesCreated.set(true);
        });

        // Async resource creation frees our application to do other things in the meantime :)
        logger.info("Doing other things while resources are being created...");
        while (!resourcesCreated.get()) doOtherThings();

        // And we pick up our database and container when they are ready.
        logger.info("Inserting 10 documents...");

        AtomicBoolean docsInserted = new AtomicBoolean();
        docsInserted.set(false);

        ExecutorService ex  = Executors.newFixedThreadPool(30);
        Scheduler customScheduler = Schedulers.fromExecutor(ex);

        int number_of_docs = 10;
        Flux.fromIterable(generateDocs(number_of_docs)) //Publisher
                .subscribeOn(customScheduler)
                .flatMap(doc -> {
                    logger.info("Sending request...");
                    // Stream operation 1: insert doc into container
                    return container.createItem(doc);
                })
                .delayElements(Duration.ofSeconds(1))
                .map(itemResponse -> {
                    // Stream operation 2: simulated network response time
                    //simulateNetworkResponseTime();

                    return itemResponse;
                })
                .flatMap(itemResponse -> {
                    // Stream operation 3: print item response
                    logger.info(String.format("Inserted item with request charge of %.2f within duration %s",
                            itemResponse.getRequestCharge(), itemResponse.getRequestLatency()));
                    return Mono.empty();
                })
                .subscribe(voidItem -> {}, err -> {},
                        () -> {
                            logger.info("Finished inserting {} documents.",number_of_docs);
                            docsInserted.set(true);
                        }
                );

        logger.info("Doing other things while docs are being inserted...");
        while (!docsInserted.get()) doOtherThings();

        logger.info("Deleting resources.");

        AtomicBoolean resourcesDeleted = new AtomicBoolean();
        resourcesDeleted.set(false);

        container.delete()
                .flatMap(containerResponse -> database.delete())
                .subscribe(dbItem -> {}, err -> {},
                        () -> {
                            logger.info("Finished deleting resources.");
                            resourcesDeleted.set(true);
                        });

        logger.info("Doing other things while deleting resources...");
        while (!resourcesDeleted.get()) doOtherThings();

        logger.info("Closing client...");

        client.close();

        logger.info("Done with demo.");

    }

    /* Intentionally exaggerated 2sec network response time for requests */
    private static void simulateNetworkResponseTime() {
        try {
            Thread.sleep(1000);
        } catch (Exception err) {
            logger.error("Simulated response time failed: ",err);
        }
    }

    /* Placeholder for background tasks to run during resource creation */
    private static void doOtherThings() {
        // Not much to do right now :)
    }

    /* Delete the resources created for this example. */
    private static void cleanup() {
        if (container != null)
            logger.info("Deleting container...");
            container.delete();

        if (database != null)
            logger.info("Deleting database...");
            database.delete();

        client.close();
    }

    /* Generate ArrayList of N unique documents (assumes /pk is id) */
    private static ArrayList<JsonNode> generateDocs(int N) {
        ArrayList<JsonNode> docs = new ArrayList<JsonNode>();
        ObjectMapper mapper = Utils.getSimpleObjectMapper();

        try {
            for (int i = 1; i <= N; i++) {
                docs.add(mapper.readTree(
                        "{" +
                                "\"id\": " +
                                "\"" + System.currentTimeMillis() + "\"" +
                                "}"
                ));

                Thread.sleep(2*i); // Unique ids w/ nonuniform spacing
            }
        } catch (Exception err) {
            logger.error("Failed generating documents: ", err);
        }

        return docs;
    }

}
