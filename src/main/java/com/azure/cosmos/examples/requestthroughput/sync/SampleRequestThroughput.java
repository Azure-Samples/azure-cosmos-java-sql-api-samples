package com.azure.cosmos.examples.requestthroughput.sync;


import com.azure.cosmos.ConnectionPolicy;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Profile;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.AtomicDouble;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class SampleRequestThroughput {

    protected static Logger logger = LoggerFactory.getLogger(SampleRequestThroughput.class.getSimpleName());

    public static void main(String[] args) {
        try {
            requestThroughputDemo();
        } catch(Exception err) {
            logger.error("Failed running demo: ", err);
        }
    }

    private static CosmosClient client;
    private static CosmosDatabase database;
    private static CosmosContainer container;
    private static AtomicInteger number_docs_inserted = new AtomicInteger(0);
    private static AtomicDouble total_charge = new AtomicDouble(0.0);
    private static int last_docs_inserted=0;
    private static double last_total_charge=0.0;
    private static double toc_time=0.0;
    private static int current_docs_inserted=0;
    private static double current_total_charge=0.0, rps=0.0, rups=0.0;

    public static void requestThroughputDemo() {
        ConnectionPolicy my_connection_policy = ConnectionPolicy.getDefaultPolicy();

        client = new CosmosClientBuilder()
                .setEndpoint(AccountSettings.HOST)
                .setKey(AccountSettings.MASTER_KEY)
                .setConnectionPolicy(ConnectionPolicy.getDefaultPolicy())
                .setConsistencyLevel(ConsistencyLevel.EVENTUAL)
                .buildClient();

        // This code synchronously sends a request to create a database.
        // While the client waits for a response, this thread is blocked from
        // performing other tasks.
        database = client.createDatabaseIfNotExists("ContosoInventoryDB").getDatabase();
        logger.info("\n\n\n\nCreated database ContosoInventoryDB.\n\n\n\n");
        CosmosContainerProperties containerProperties = new CosmosContainerProperties("ContosoInventoryContainer", "/id");
        container = database.createContainerIfNotExists(containerProperties, 100000).getContainer();
        logger.info("\n\n\n\nCreated container ContosoInventoryContainer.\n\n\n\n");
        // Resources are ready.
        //
        // Create many docs to insert into the container
        int number_of_docs = 4000000;
        logger.info("Generating {} documents...", number_of_docs);
        ArrayList<JsonNode> docs = Profile.generateDocs(number_of_docs);
        logger.info("Inserting {} documents...", number_of_docs);

        Profile.tic();

        // Insert many docs synchronously.
        // The client blocks waiting for a response to each insert request,
        // which limits throughput.
        // While the client is waiting for a response, the thread is blocked from other tasks
        for(JsonNode doc : docs) {
            CosmosItemResponse<JsonNode> itemResponse = container.createItem(doc);
            if (itemResponse.getStatusCode() == 201) {
                number_docs_inserted.getAndIncrement();
                total_charge.getAndAdd(itemResponse.getRequestCharge());
            }
            else
                logger.warn("WARNING insert status code {} != 201", itemResponse.getStatusCode());
        }

        //Profiler code - it's good for this part to be async
        Flux.interval(Duration.ofMillis(10)).take(180000).flatMap(tick -> {
            toc_time=Profile.toc_ms();
            current_docs_inserted=number_docs_inserted.get();
            current_total_charge=total_charge.get();
            if (toc_time >= 1000.0) {
                Profile.tic();
                rps=1000.0*((double)(current_docs_inserted-last_docs_inserted))/toc_time;
                rups=1000.0*(current_total_charge-last_total_charge)/toc_time;
                logger.info(String.format("\n\n\n\n" +
                        "Sync Throughput Profiler Result, Last 1000ms:" + "\n\n" +
                        "%8s          %8s", StringUtils.center("Req/sec",8),StringUtils.center("RU/s",8)) + "\n"
                        + "----------------------------------" + "\n"
                        + String.format("%8.1f          %8.1f",rps,rups) + "\n\n\n\n");
                last_docs_inserted=current_docs_inserted;
                last_total_charge=current_total_charge;
            }
            return Mono.empty();
        }).blockLast();
        System.out.println("Done.");
        while (true);

        /*

        // Clean up
        logger.info("Deleting resources.");
        container.delete();
        database.delete();
        logger.info("Finished deleting resources.");

        logger.info("Closing client...");
        client.close();

        logger.info("Done with demo.");

         */
    }
}
