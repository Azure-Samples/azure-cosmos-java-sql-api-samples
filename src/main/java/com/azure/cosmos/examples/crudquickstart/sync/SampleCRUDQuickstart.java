// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.crudquickstart.sync;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Families;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SampleCRUDQuickstart {

    private CosmosClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";

    private CosmosDatabase database;
    private CosmosContainer container;

    protected static Logger logger = LoggerFactory.getLogger(SampleCRUDQuickstart.class);

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
    //  <Main>
    public static void main(String[] args) {
        SampleCRUDQuickstart p = new SampleCRUDQuickstart();

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

    //  </Main>

    private void getStartedDemo() throws Exception {

        logger.info("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        ArrayList<String> preferredRegions = new ArrayList<String>();
        preferredRegions.add("West US");

        //  Create sync client
        //  <CreateSyncClient>
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .preferredRegions(preferredRegions)
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .contentResponseOnWriteEnabled(true)
                .buildClient();

        //  </CreateSyncClient>

        createDatabaseIfNotExists();
        createContainerIfNotExists();

        //  Setup family items to create
        ArrayList<Family> familiesToCreate = new ArrayList<>();
        familiesToCreate.add(Families.getAndersenFamilyItem());
        familiesToCreate.add(Families.getWakefieldFamilyItem());
        familiesToCreate.add(Families.getJohnsonFamilyItem());
        familiesToCreate.add(Families.getSmithFamilyItem());

        // Creates several items in the container
        // Also applies an upsert operation to one of the items (create if not present, otherwise replace)
        createFamilies(familiesToCreate);

        logger.info("Reading items.");
        readItems(familiesToCreate);

        logger.info("Replacing items.");
        replaceItems(familiesToCreate);

        logger.info("Querying items.");
        queryItems();

        logger.info("Delete an item.");
        deleteItem(familiesToCreate.get(0));
    }

    private void createDatabaseIfNotExists() throws Exception {
        logger.info("Create database " + databaseName + " if not exists.");

        //  Create database if not exists
        //  <CreateDatabaseIfNotExists>
        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName);
        database = client.getDatabase(databaseResponse.getProperties().getId());

        //  </CreateDatabaseIfNotExists>

        logger.info("Checking database " + database.getId() + " completed!\n");
    }

    private void createContainerIfNotExists() throws Exception {
        logger.info("Create container " + containerName + " if not exists.");

        //  Create container if not exists
        //  <CreateContainerIfNotExists>
        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(containerName, "/lastName");

        //  Create container with 400 RU/s
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);
        CosmosContainerResponse containerResponse = database.createContainerIfNotExists(containerProperties, throughputProperties);
        container = database.getContainer(containerResponse.getProperties().getId());
        //  </CreateContainerIfNotExists>

        logger.info("Checking container " + container.getId() + " completed!\n");
    }

    private void createFamilies(List<Family> families) throws Exception {
        double totalRequestCharge = 0;
        for (Family family : families) {

            //  <CreateItem>
            //  Create item using container that we created using sync client

            //  Use lastName as partitionKey for cosmos item
            //  Using appropriate partition key improves the performance of database operations
            CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions();
            CosmosItemResponse<Family> item = container.createItem(family, new PartitionKey(family.getLastName()), cosmosItemRequestOptions);
            //  </CreateItem>

            //  Get request charge and other properties like latency, and diagnostics strings, etc.
            logger.info(String.format("Created item with request charge of %.2f within duration %s",
                    item.getRequestCharge(), item.getDuration()));

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
                item.getRequestCharge(), item.getDuration()));
    }

    private void readItems(ArrayList<Family> familiesToCreate) {
        //  Using partition key for point read scenarios.
        //  This will help fast look up of items because of partition key
        familiesToCreate.forEach(family -> {
            //  <ReadItem>
            try {
                CosmosItemResponse<Family> item = container.readItem(family.getId(), new PartitionKey(family.getLastName()), Family.class);
                double requestCharge = item.getRequestCharge();
                Duration requestLatency = item.getDuration();
                logger.info(String.format("Item successfully read with id %s with a charge of %.2f and within duration %s",
                        item.getItem().getId(), requestCharge, requestLatency));
            } catch (CosmosException e) {
                e.printStackTrace();
                logger.info(String.format("Read Item failed with %s", e));
            }
            //  </ReadItem>
        });
    }

    private void replaceItems(ArrayList<Family> familiesToCreate) {
        familiesToCreate.forEach(family -> {
            //  <ReadItem>
            try {
                String district = family.getDistrict();
                family.setDistrict(district + "_newDistrict");
                CosmosItemResponse<Family> item = container.replaceItem(family, family.getId(),
                    new PartitionKey(family.getLastName()), new CosmosItemRequestOptions());
                double requestCharge = item.getRequestCharge();
                Duration requestLatency = item.getDuration();
                logger.info("Item successfully replaced with id: {}, district: {}, charge: {}, duration: {}",
                    item.getItem().getId(), item.getItem().getDistrict(), requestCharge, requestLatency);
            } catch (CosmosException e) {
                logger.error(String.format("Replace Item failed with %s", e));
            }
            //  </ReadItem>
        });
    }

    private void queryItems() {
        //  <QueryItems>

        // Set some common query options
        int preferredPageSize = 10;
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        //queryOptions.setEnableCrossPartitionQuery(true); //No longer necessary in SDK v4
        //  Set populate query metrics to get metrics around query executions
        queryOptions.setQueryMetricsEnabled(true);

        CosmosPagedIterable<Family> familiesPagedIterable = container.queryItems(
                "SELECT * FROM Family WHERE Family.lastName IN ('Andersen', 'Wakefield', 'Johnson')", queryOptions, Family.class);

        familiesPagedIterable.iterableByPage(preferredPageSize).forEach(cosmosItemPropertiesFeedResponse -> {
            logger.info("Got a page of query result with " +
                    cosmosItemPropertiesFeedResponse.getResults().size() + " items(s)"
                    + " and request charge of " + cosmosItemPropertiesFeedResponse.getRequestCharge());

            logger.info("Item Ids " + cosmosItemPropertiesFeedResponse
                    .getResults()
                    .stream()
                    .map(Family::getId)
                    .collect(Collectors.toList()));
        });
        //  </QueryItems>
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
