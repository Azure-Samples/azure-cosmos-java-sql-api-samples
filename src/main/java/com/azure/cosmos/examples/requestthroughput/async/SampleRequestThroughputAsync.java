package com.azure.cosmos.examples.requestthroughput.async;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Profile;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.ThroughputProperties;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/*
 * Async Request Throughput Sample
 *
 * Please note that perf testing incurs costs for provisioning container throughput and storage.
 *
 * This throughput profiling sample issues high-throughput document insert requests to an Azure Cosmos DB container.
 * Run this code in a geographically colocated VM for best performance.
 *
 * Example configuration
 * -Provision 100000 RU/s container throughput
 * -Generate 4M documents
 * -Result: ~60K RU/s actual throughput
 */

public class SampleRequestThroughputAsync {

    protected static Logger logger = LoggerFactory.getLogger(SampleRequestThroughputAsync.class);

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
    private static AtomicInteger total_charge = new AtomicInteger(0);

    public static void requestThroughputDemo() {

        // Create Async client.
        // Building an async client is still a sync operation.
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .contentResponseOnWriteEnabled(true)
                .buildAsyncClient();

        // Describe the logic of database and container creation using Reactor...
        Mono<Void> databaseContainerIfNotExist = client.createDatabaseIfNotExists("ContosoInventoryDB").flatMap(databaseResponse -> {
            database = client.getDatabase(databaseResponse.getProperties().getId());
            logger.info("\n\n\n\nCreated database ContosoInventoryDB.\n\n\n\n");
            CosmosContainerProperties containerProperties = new CosmosContainerProperties("ContosoInventoryContainer", "/id");
            ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);
            return database.createContainerIfNotExists(containerProperties, throughputProperties);
        }).flatMap(containerResponse -> {
            container = database.getContainer(containerResponse.getProperties().getId());
            logger.info("\n\n\n\nCreated container ContosoInventoryContainer.\n\n\n\n");
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
        int number_of_docs = 50000;
        logger.info("Generating {} documents...", number_of_docs);
        ArrayList<JsonNode> docs = Profile.generateDocs(number_of_docs);

        // Insert many docs into container...
        logger.info("Inserting {} documents...", number_of_docs);

        Profile.tic();
        int last_docs_inserted=0;
        double last_total_charge=0.0;

        Flux.fromIterable(docs).flatMap(doc -> container.createItem(doc))
                // ^Publisher: upon subscription, createItem inserts a doc &
                // publishes request response to the next operation...
                .flatMap(itemResponse -> {
                    // ...Streaming operation: count each doc & check success...

                    if (itemResponse.getStatusCode() == 201) {
                        number_docs_inserted.getAndIncrement();
                        total_charge.getAndAdd((int)(itemResponse.getRequestCharge()));
                    }
                    else
                        logger.warn("WARNING insert status code {} != 201", itemResponse.getStatusCode());
                    return Mono.empty();
                }).subscribe(); // ...Subscribing to the publisher triggers stream execution.

        // Do other things until async response arrives
        logger.info("Doing other things until async doc inserts complete...");
        //while (number_docs_inserted.get() < number_of_docs) Profile.doOtherThings();
        double toc_time=0.0;
        int current_docs_inserted=0;
        double current_total_charge=0.0, rps=0.0, rups=0.0;
        while (number_docs_inserted.get() < number_of_docs) {
            toc_time=Profile.toc_ms();
            current_docs_inserted=number_docs_inserted.get();
            current_total_charge=total_charge.get();
            if (toc_time >= 1000.0) {
                Profile.tic();
                rps=1000.0*((double)(current_docs_inserted-last_docs_inserted))/toc_time;
                rups=1000.0*(current_total_charge-last_total_charge)/toc_time;
                logger.info(String.format("\n\n\n\n" +
                        "Async Throughput Profiler Result, Last 1000ms:" + "\n\n" +
                        "%8s          %8s", StringUtils.center("Req/sec",8),StringUtils.center("RU/s",8)) + "\n"
                        + "----------------------------------" + "\n"
                        + String.format("%8.1f          %8.1f",rps,rups) + "\n\n\n\n");
                last_docs_inserted=current_docs_inserted;
                last_total_charge=current_total_charge;
            }
        }

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
