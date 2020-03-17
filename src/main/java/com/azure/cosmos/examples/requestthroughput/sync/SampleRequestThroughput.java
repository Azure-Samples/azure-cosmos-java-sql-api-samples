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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

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
        logger.info("Got DB.");
        CosmosContainerProperties containerProperties = new CosmosContainerProperties("ContosoInventoryContainer", "/id");
        container = database.createContainerIfNotExists(containerProperties, 400).getContainer();
        logger.info("Got container.");
        // Resources are ready.
        //
        // Create many docs to insert into the container
        int number_of_docs = 4000000;
        logger.info("Generating {} documents...", number_of_docs);
        ArrayList<JsonNode> docs = Profile.generateDocs(number_of_docs);
        logger.info("Inserting {} documents...", number_of_docs);

        // Insert many docs synchronously.
        // The client blocks waiting for a response to each insert request,
        // which limits throughput.
        // While the client is waiting for a response, the thread is blocked from other tasks
        docs.forEach(doc -> {
            CosmosItemResponse<JsonNode> itemResponse = container.createItem(doc);
            if (itemResponse.getStatusCode() != 201)
                logger.warn("WARNING insert status code {} != 201", itemResponse.getStatusCode());
        });

        // Clean up
        logger.info("Deleting resources.");
        container.delete();
        database.delete();
        logger.info("Finished deleting resources.");

        logger.info("Closing client...");
        client.close();

        logger.info("Done with demo.");
    }
}
