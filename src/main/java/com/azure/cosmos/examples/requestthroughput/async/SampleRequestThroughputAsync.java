package com.azure.cosmos.examples.requestthroughput.async;

import com.azure.cosmos.ConnectionPolicy;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Profile;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SampleRequestThroughputAsync {

    protected static Logger logger = LoggerFactory.getLogger(SampleRequestThroughputAsync.class.getSimpleName());

    public static void main(String[] args) {
        try {
            requestThroughputDemo();
        } catch(Exception err) {
            logger.error("Failed running demo: ", err);
        }
    }

    private static CosmosAsyncClient client;
    private static CosmosAsyncDatabase database;
    private static CosmosAsyncContainer container;
    private static AtomicBoolean resources_created = new AtomicBoolean(false);
    private static AtomicInteger number_docs_inserted = new AtomicInteger(0);
    private static AtomicBoolean resources_deleted = new AtomicBoolean(false);

    public static void requestThroughputDemo() {
        ConnectionPolicy my_connection_policy = ConnectionPolicy.getDefaultPolicy();

        // Create Async client.
        // Building an async client is still a sync operation.
        client = new CosmosClientBuilder()
                .setEndpoint(AccountSettings.HOST)
                .setKey(AccountSettings.MASTER_KEY)
                .setConnectionPolicy(ConnectionPolicy.getDefaultPolicy())
                .setConsistencyLevel(ConsistencyLevel.EVENTUAL)
                .buildAsyncClient();

        // Describe the logic of database and container creation using Reactor...
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

        // ...it doesn't execute until you subscribe().
        // The async call returns immediately...
        logger.info("Creating database and container asynchronously...");
        databaseContainerIfNotExist.subscribe(voidItem -> {}, err -> {},
                () -> {
                    logger.info("Finished creating resources.\n\n");
                    resources_created.set(true);
        });

        // ...so we can do other things until async response arrives!
        logger.info("Doing other things until async resource creation completes......");
        while (!resources_created.get()) Profile.doOtherThings();

        // Container is created. Generate many docs to insert.
        int number_of_docs = 4000000;
        logger.info("Generating {} documents...", number_of_docs);
        ArrayList<JsonNode> docs = Profile.generateDocs(number_of_docs);

        // Insert many docs into container...
        logger.info("Inserting {} documents...", number_of_docs);
        docs.forEach(doc -> {
            try {
                Thread.sleep(12);
            } catch (Exception err) {
                logger.error("Error throttling programmatically: ",err);
            }
            // ...by describing logic of item insertion using Reactor. Then subscribe() to execute.
            container.createItem(doc)
                    // ^Publisher: upon subscription, createItem inserts a doc &
                    // publishes request response to the next operation...
                    .flatMap(itemResponse -> {
                        // ...Streaming operation: count each doc & check success...
                        if (itemResponse.getStatusCode() == 201)
                            number_docs_inserted.getAndIncrement();
                        else
                            logger.warn("WARNING insert status code {} != 201", itemResponse.getStatusCode());
                        return Mono.empty();
                    }).subscribe(); // ...Subscribing to the publisher triggers stream execution.
        });

        // Do other things until async response arrives
        logger.info("Doing other things until async doc inserts complete...");
        while (number_docs_inserted.get() < number_of_docs) Profile.doOtherThings();

        // Inserts are complete. Cleanup (asynchronously!)
        logger.info("Deleting resources.");
        container.delete()
                .flatMap(containerResponse -> database.delete())
                .subscribe(dbItem -> {}, err -> {},
                        () -> {
                            logger.info("Finished deleting resources.");
                            resources_deleted.set(true);
                        });

        // Do other things until async response arrives
        logger.info("Do other things until async resource delete completes...");
        while (!resources_deleted.get()) Profile.doOtherThings();

        // Close client. This is always sync.
        logger.info("Closing client...");
        client.close();
        logger.info("Done with demo.");
    }
}
