// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.ttl.sync;

import com.azure.cosmos.ConnectionPolicy;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosClientException;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosPagedIterable;
import com.azure.cosmos.examples.changefeed.SampleChangeFeedProcessor;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.CustomPOJO;
import com.azure.cosmos.examples.common.Families;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.examples.common.SimpleFamilyMemberPOJO;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.FeedOptions;
import com.azure.cosmos.models.PartitionKey;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class SampleTTLQuickstart {

    private CosmosClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";

    private CosmosDatabase database;
    private CosmosContainer container;

    protected static Logger logger = LoggerFactory.getLogger(SampleChangeFeedProcessor.class.getSimpleName());

    public void close() {
        client.close();
    }

    /**
     * Run a Hello CosmosDB console application.
     * <p>
     * This is a simple sample application intended to demonstrate Create, Read, Update, Delete (CRUD) operations
     * with Azure Cosmos DB Java SDK, as applied to databases, containers and items. This sample will
     * 1. Create synchronous client, database and container instances
     * 2. Create several items
     * 3. Upsert one of the items
     * 4. Perform a query over the items
     * 5. Delete an item
     * 6. Delete the Cosmos DB database and container resources and close the client.     *
     */
    public static void main(String[] args) {
        SampleTTLQuickstart p = new SampleTTLQuickstart();

        try {
            logger.info("Starting SYNC main");
            p.getStartedDemo();
            logger.info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(String.format("Cosmos getStarted failed with %s", e));
        } finally {
            logger.info("Closing the client");
            p.shutdown();
        }
    }


    private void getStartedDemo() throws Exception {

        logger.info("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        ConnectionPolicy defaultPolicy = ConnectionPolicy.getDefaultPolicy();
        //  Setting the preferred location to Cosmos DB Account region
        //  West US is just an example. User should set preferred location to the Cosmos DB region closest to the application
        defaultPolicy.setPreferredLocations(Lists.newArrayList("West US"));

        //  Create sync client
        client = new CosmosClientBuilder()
                .setEndpoint(AccountSettings.HOST)
                .setKey(AccountSettings.MASTER_KEY)
                .setConnectionPolicy(defaultPolicy)
                .setConsistencyLevel(ConsistencyLevel.EVENTUAL)
                .buildClient();


        createDatabaseIfNotExists();
        createContainerIfNotExists();

        //  Enable TTL for items in a container but do not set a default TTL
        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, "/lastName");
        containerProperties.setDefaultTimeToLiveInSeconds(-1);
        container.replace(containerProperties);

        // Enable TTL for items in a container and set a default TTL
        containerProperties.setDefaultTimeToLiveInSeconds(90 * 60 * 60 * 24);
        container.replace(containerProperties);

        // Set item TTL as a POJO field
        SimpleFamilyMemberPOJO famPOJO = new SimpleFamilyMemberPOJO();
        famPOJO.setLastName("Andrews");
        famPOJO.setId(UUID.randomUUID().toString());
        famPOJO.setTtl(5); //5 sec TTL
        container.createItem(famPOJO, new PartitionKey(famPOJO.getLastName()), new CosmosItemRequestOptions());

        //  Reset time-to-live - TTL value stays the same, this just resets the timer (reset _ts)
        CosmosItemResponse<SimpleFamilyMemberPOJO> itemResponseReset = container.upsertItem(famPOJO, new CosmosItemRequestOptions());

        //  Remove (null) the item TTL field to accept container default.
        famPOJO.setTtl(null);
        container.upsertItem(famPOJO, new CosmosItemRequestOptions());

        //  Set the item TTL field to -1 to disable TTL for that item
        famPOJO.setTtl(-1);
        container.upsertItem(famPOJO, new CosmosItemRequestOptions());

        //  Disable TTL on the container by deleting (nulling) the property
        containerProperties.setDefaultTimeToLiveInSeconds(null);
        container.replace(containerProperties);
    }

    private void createDatabaseIfNotExists() throws Exception {
        logger.info("Create database " + databaseName + " if not exists.");

        //  Create database if not exists
        database = client.createDatabaseIfNotExists(databaseName).getDatabase();

        logger.info("Checking database " + database.getId() + " completed!\n");
    }

    private void createContainerIfNotExists() throws Exception {
        logger.info("Create container " + containerName + " if not exists.");

        //  Create container if not exists
        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(containerName, "/lastName");

        //  Create container with 400 RU/s
        container = database.createContainerIfNotExists(containerProperties, 400).getContainer();

        logger.info("Checking container " + container.getId() + " completed!\n");
    }

    private void createFamilies(List<Family> families) throws Exception {
        double totalRequestCharge = 0;
        for (Family family : families) {

            //  Create item using container that we created using sync client

            //  Use lastName as partitionKey for cosmos item
            //  Using appropriate partition key improves the performance of database operations
            CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions();
            CosmosItemResponse<Family> item = container.createItem(family, new PartitionKey(family.getLastName()), cosmosItemRequestOptions);

            //  Get request charge and other properties like latency, and diagnostics strings, etc.
            logger.info(String.format("Created item with request charge of %.2f within duration %s",
                    item.getRequestCharge(), item.getRequestLatency()));

            totalRequestCharge += item.getRequestCharge();
        }
        logger.info(String.format("Created %d items with total request charge of %.2f",
                families.size(), totalRequestCharge));

        Family family_to_upsert = families.get(0);
        logger.info(String.format("Upserting the item with id %s after modifying the isRegistered field...", family_to_upsert.getId()));
        family_to_upsert.setRegistered(!family_to_upsert.isRegistered());

        CosmosItemResponse<Family> item = container.upsertItem(family_to_upsert);

        //  Get upsert request charge and other properties like latency, and diagnostics strings, etc.
        logger.info(String.format("Upserted item with request charge of %.2f within duration %s",
                item.getRequestCharge(), item.getRequestLatency()));
    }

    private void readItems(ArrayList<Family> familiesToCreate) {
        //  Using partition key for point read scenarios.
        //  This will help fast look up of items because of partition key
        familiesToCreate.forEach(family -> {
            try {
                CosmosItemResponse<Family> item = container.readItem(family.getId(), new PartitionKey(family.getLastName()), Family.class);
                double requestCharge = item.getRequestCharge();
                Duration requestLatency = item.getRequestLatency();
                logger.info(String.format("Item successfully read with id %s with a charge of %.2f and within duration %s",
                        item.getResource().getId(), requestCharge, requestLatency));
            } catch (CosmosClientException e) {
                e.printStackTrace();
                logger.info(String.format("Read Item failed with %s", e));
            }
        });
    }

    private void queryItems() {
        // Set some common query options
        FeedOptions queryOptions = new FeedOptions();
        queryOptions.setMaxItemCount(10);
        //queryOptions.setEnableCrossPartitionQuery(true); //No longer necessary in SDK v4
        //  Set populate query metrics to get metrics around query executions
        queryOptions.setPopulateQueryMetrics(true);

        CosmosPagedIterable<Family> familiesPagedIterable = container.queryItems(
                "SELECT * FROM Family WHERE Family.lastName IN ('Andersen', 'Wakefield', 'Johnson')", queryOptions, Family.class);

        familiesPagedIterable.iterableByPage().forEach(cosmosItemPropertiesFeedResponse -> {
            logger.info("Got a page of query result with " +
                    cosmosItemPropertiesFeedResponse.getResults().size() + " items(s)"
                    + " and request charge of " + cosmosItemPropertiesFeedResponse.getRequestCharge());

            logger.info("Item Ids " + cosmosItemPropertiesFeedResponse
                    .getResults()
                    .stream()
                    .map(Family::getId)
                    .collect(Collectors.toList()));
        });
    }

    private void deleteItem(Family item) {
        container.deleteItem(item.getId(), new PartitionKey(item.getLastName()), new CosmosItemRequestOptions());
    }

    private void shutdown() {
        try {
            //Clean shutdown
            logger.info("Deleting Cosmos DB resources");
            logger.info("-Deleting container...");
            if (container != null)
                container.delete();
            logger.info("-Deleting database...");
            if (database != null)
                database.delete();
            logger.info("-Closing the client...");
        } catch (Exception err) {
            logger.error("Deleting Cosmos DB resources failed, will still attempt to close the client. See stack trace below.");
            err.printStackTrace();
        }
        client.close();
        logger.info("Done.");
    }
}