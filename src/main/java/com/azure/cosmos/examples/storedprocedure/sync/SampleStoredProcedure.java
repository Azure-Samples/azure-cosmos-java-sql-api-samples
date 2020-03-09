// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.storedprocedure.sync;

import com.azure.cosmos.*;
import com.azure.cosmos.examples.changefeed.SampleChangeFeedProcessor;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.CustomPOJO;
import com.azure.cosmos.implementation.Utils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
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
     * Run a Hello CosmosDB console application.
     *
     * @param args command line args.
     */
    //  <Main>
    public static void main(String[] args) {
        SampleStoredProcedure p = new SampleStoredProcedure();

        try {
            p.sprocDemo();
            System.out.println("Demo complete, please hold while resources are released");
            p.shutdown();
            System.out.println("Done.\n");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(String.format("Cosmos getStarted failed with %s", e));
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
            System.out.println("Checking that a document was created by the stored procedure...");
            CosmosItemResponse<CustomPOJO> test_resp = container.readItem("test_doc",new PartitionKey("test_doc"),CustomPOJO.class);
            System.out.println(String.format(
                    "Result of point-read for document created by stored procedure (200 indicates success): %d",test_resp.getStatusCode()));
    }

    public void setUp() throws Exception{
        System.out.println("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

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

            System.out.println("Create database " + databaseName + " with container " + containerName + " if either does not already exist.\n");

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
        System.out.println("Creating stored procedure...");

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
        CosmosStoredProcedureProperties storedProcedureDef =  new CosmosStoredProcedureProperties(sprocId,sprocBody);
        container.getScripts()
            .createStoredProcedure(storedProcedureDef,
            new CosmosStoredProcedureRequestOptions());
    }

    private void readAllSprocs() throws Exception {
        System.out.println("Listing all stored procedures associated with container " + containerName + "\n");

        FeedOptions feedOptions = new FeedOptions();
        CosmosContinuablePagedIterable<CosmosStoredProcedureProperties> feedResponseIterable =
                container.getScripts().readAllStoredProcedures(feedOptions);

        Iterator<CosmosStoredProcedureProperties> feedResponseIterator = feedResponseIterable.iterator();

        while(feedResponseIterator.hasNext()) {
            CosmosStoredProcedureProperties storedProcedureProperties = feedResponseIterator.next();
            System.out.println(String.format("Stored Procedure: %s",storedProcedureProperties));
        }
    }

    public void executeStoredProcedure() throws Exception {
        System.out.println(String.format("Executing stored procedure %s...\n\n",sprocId));

        CosmosStoredProcedureRequestOptions options = new CosmosStoredProcedureRequestOptions();
        options.setPartitionKey(new PartitionKey("test_doc"));
        CosmosStoredProcedureResponse executeResponse = container.getScripts()
                                                         .getStoredProcedure(sprocId)
                                                         .execute(null, options);

        System.out.println(String.format("Stored procedure %s returned %s (HTTP %d), at cost %.3f RU.\n",
                                         sprocId,
                                         executeResponse.responseAsString(),
                                         executeResponse.getStatusCode(),
                                         executeResponse.getRequestCharge()));
    }

    public void deleteStoredProcedure() throws Exception {
        System.out.println("-Deleting stored procedure...\n");
        container.getScripts()
            .getStoredProcedure(sprocId)
            .delete();
        System.out.println("-Deleting database...\n");
        database.delete();
        System.out.println("-Closing client instance...\n");
        client.close();
        System.out.println("Done.");
    }
}
