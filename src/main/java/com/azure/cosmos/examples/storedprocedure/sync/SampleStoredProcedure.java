// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.storedprocedure.sync;

import com.azure.cosmos.ConnectionPolicy;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosPagedIterable;
import com.azure.cosmos.examples.changefeed.SampleChangeFeedProcessor;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.CustomPOJO;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosStoredProcedureProperties;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureResponse;
import com.azure.cosmos.models.FeedOptions;
import com.azure.cosmos.models.PartitionKey;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class SampleStoredProcedure {

    private CosmosClient client;

    private final String databaseName = "SprocTestDB";
    private final String containerName = "SprocTestContainer";

    private CosmosDatabase database;
    private CosmosContainer container;

    private String sprocId;

    protected static Logger logger = LoggerFactory.getLogger(SampleChangeFeedProcessor.class.getSimpleName());

    public void close() {
        client.close();
    }

    /**
     * Stored Procedure Example
     * <p>
     * This sample code demonstrates creation, execution, and effects of stored procedures
     * using Java SDK. A stored procedure is created which will insert a JSON object into
     * a Cosmos DB container. The sample executes the stored procedure and then performs
     * a point-read to confirm that the stored procedure had the intended effect.
     */
    //  <Main>
    public static void main(String[] args) {
        SampleStoredProcedure p = new SampleStoredProcedure();

        try {
            p.sprocDemo();
            logger.info("Demo complete, please hold while resources are released");
            p.shutdown();
            logger.info("Done.\n");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(String.format("Cosmos getStarted failed with %s", e));
            p.close();
        } finally {
        }
    }

    //  </Main>

    private void sprocDemo() throws Exception {
        //Setup client, DB, and the container for which we will create stored procedures
        //The container partition key will be id
        setUp();

        //Create stored procedure and list all stored procedures that have been created.
        createStoredProcedure();
        readAllSprocs();

        //Execute the stored procedure, which we expect will create an item with id test_doc
        executeStoredProcedure();

        //Perform a point-read to confirm that the item with id test_doc exists
        logger.info("Checking that a document was created by the stored procedure...");
        CosmosItemResponse<CustomPOJO> test_resp = container.readItem("test_doc", new PartitionKey("test_doc"), CustomPOJO.class);
        logger.info(String.format(
                "Result of point-read for document created by stored procedure (200 indicates success): %d", test_resp.getStatusCode()));
    }

    public void setUp() throws Exception {
        logger.info("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        ConnectionPolicy defaultPolicy = ConnectionPolicy.getDefaultPolicy();
        //  Setting the preferred location to Cosmos DB Account region
        //  West US is just an example. User should set preferred location to the Cosmos DB region closest to the application
        defaultPolicy.setPreferredLocations(Lists.newArrayList("West US"));

        //  Create sync client
        //  <CreateSyncClient>
        client = new CosmosClientBuilder()
                .setEndpoint(AccountSettings.HOST)
                .setKey(AccountSettings.MASTER_KEY)
                .setConnectionPolicy(defaultPolicy)
                .setConsistencyLevel(ConsistencyLevel.EVENTUAL)
                .buildClient();

        logger.info("Create database " + databaseName + " with container " + containerName + " if either does not already exist.\n");

        database = client.createDatabaseIfNotExists(databaseName).getDatabase();

        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(containerName, "/id");
        container = database.createContainerIfNotExists(containerProperties, 400).getContainer();
    }

    public void shutdown() throws Exception {
        //Safe clean & close
        deleteStoredProcedure();
    }

    public void createStoredProcedure() throws Exception {
        logger.info("Creating stored procedure...");

        sprocId = "createMyDocument";
        String sprocBody = "function createMyDocument() {\n" +
                "var documentToCreate = {\"id\":\"test_doc\"}\n" +
                "var context = getContext();\n" +
                "var collection = context.getCollection();\n" +
                "var accepted = collection.createDocument(collection.getSelfLink(), documentToCreate,\n" +
                "    function (err, documentCreated) {\n" +
                "if (err) throw new Error('Error' + err.message);\n" +
                "context.getResponse().setBody(documentCreated.id)\n" +
                "});\n" +
                "if (!accepted) return;\n" +
                "}";
        CosmosStoredProcedureProperties storedProcedureDef = new CosmosStoredProcedureProperties(sprocId, sprocBody);
        container.getScripts()
                .createStoredProcedure(storedProcedureDef,
                        new CosmosStoredProcedureRequestOptions());
    }

    private void readAllSprocs() throws Exception {
        logger.info("Listing all stored procedures associated with container " + containerName + "\n");

        FeedOptions feedOptions = new FeedOptions();
        CosmosPagedIterable<CosmosStoredProcedureProperties> feedResponseIterable =
                container.getScripts().readAllStoredProcedures(feedOptions);

        Iterator<CosmosStoredProcedureProperties> feedResponseIterator = feedResponseIterable.iterator();

        while (feedResponseIterator.hasNext()) {
            CosmosStoredProcedureProperties storedProcedureProperties = feedResponseIterator.next();
            logger.info(String.format("Stored Procedure: %s", storedProcedureProperties));
        }
    }

    public void executeStoredProcedure() throws Exception {
        logger.info(String.format("Executing stored procedure %s...\n\n", sprocId));

        CosmosStoredProcedureRequestOptions options = new CosmosStoredProcedureRequestOptions();
        options.setPartitionKey(new PartitionKey("test_doc"));
        CosmosStoredProcedureResponse executeResponse = container.getScripts()
                .getStoredProcedure(sprocId)
                .execute(null, options);

        logger.info(String.format("Stored procedure %s returned %s (HTTP %d), at cost %.3f RU.\n",
                sprocId,
                executeResponse.responseAsString(),
                executeResponse.getStatusCode(),
                executeResponse.getRequestCharge()));
    }

    public void deleteStoredProcedure() throws Exception {
        logger.info("-Deleting stored procedure...\n");
        container.getScripts()
                .getStoredProcedure(sprocId)
                .delete();
        logger.info("-Deleting database...\n");
        database.delete();
        logger.info("-Closing client instance...\n");
        client.close();
        logger.info("Done.");
    }
}
