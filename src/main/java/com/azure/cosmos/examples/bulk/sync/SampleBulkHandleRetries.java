// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.bulk.sync;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.ThrottlingRetryOptions;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.models.CosmosContainerRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.identity.DefaultAzureCredentialBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SampleBulkHandleRetries {

    private static final Logger logger = LoggerFactory.getLogger(SampleBulkHandleRetries.class);
    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";
    private CosmosClient client;
    private CosmosDatabase database;
    private CosmosContainer container;
    private int noOfItemsToCreate = 500;

    public static void main(String[] args) {
        SampleBulkHandleRetries p = new SampleBulkHandleRetries();

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

    public void close() {
        client.close();
    }

    private void getStartedDemo() {

        logger.info("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        /* Create an async client
        force rate limiting by reducing the max retry attempts
        to simulate throttling when under heavy load*/
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .credential(new DefaultAzureCredentialBuilder().build())
                .throttlingRetryOptions(
                        new ThrottlingRetryOptions()
                                .setMaxRetryAttemptsOnThrottledRequests(1)
                                .setMaxRetryWaitTime(Duration.ofSeconds(1)))
                .contentResponseOnWriteEnabled(true)
                .consistencyLevel(ConsistencyLevel.SESSION).buildClient();

        //  </CreateAsyncClient>

        // Note: database must already exist (cannot be created via RBAC).
        database = client.getDatabase(databaseName);
        createContainerIfNotExists();

        // Create 500 Family records
        ArrayList<Family> largeFamilies1 = new ArrayList<>();
        for (int i = 0; i < noOfItemsToCreate; i++) {
            Family family = new Family();
            family.setId(UUID.randomUUID().toString());
            family.setLastName("Family0");
            family.setRegistered(false);
            largeFamilies1.add(family);
        }
        logger.info("Ensure rate limiting with enough bulk upserts, retries handled by BulkWriter abstraction");
        largeBulkUpsertItemsWithBulkWriterAbstraction(largeFamilies1);
    }

    private void createContainerIfNotExists() {
        logger.info("Create container " + containerName + " if not exists.");

        //  Create container if not exists
        //  <CreateContainerIfNotExists>

        CosmosContainerProperties containerProperties = new CosmosContainerProperties(
                containerName, "/lastName");
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);
        CosmosContainerResponse containerIfNotExists;
        containerIfNotExists = database
                .createContainerIfNotExists(containerProperties, throughputProperties);

        //  Create container with 400 RU/s
        CosmosContainerResponse cosmosContainerResponse = containerIfNotExists;
        assert (cosmosContainerResponse != null);
        assert (cosmosContainerResponse.getProperties() != null);
        container = database.getContainer(cosmosContainerResponse.getProperties().getId());
        //  </CreateContainerIfNotExists>

        //Modify existing container
        containerProperties = cosmosContainerResponse.getProperties();
        container.replace(containerProperties, new CosmosContainerRequestOptions());

        logger.info(
                "setupContainer(): Container {}} in {} has been updated with it's new properties.",
                container.getId(),
                database.getId());

    }


    private void largeBulkUpsertItemsWithBulkWriterAbstraction(Iterable<Family> families) {
        List<CosmosItemOperation> cosmosItemOperations = new ArrayList<>();
        for (Family family : families) {
            cosmosItemOperations.add(CosmosBulkOperations.getUpsertItemOperation(family, new PartitionKey(family.getLastName())));
        }
        com.azure.cosmos.examples.bulk.sync.BulkWriter bulkWriter = new BulkWriter(container);
        for (CosmosItemOperation operation : cosmosItemOperations) {
            bulkWriter.scheduleWrites(operation);
        }
        bulkWriter.execute();
        //get count of items in container
        try {
            logger.info("Waiting for bulk operations to complete...");
            Thread.sleep(20000); // Wait for the bulk operations to complete
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        logger.info("Number of items to create was: " + noOfItemsToCreate);
        logger.info("Total items created after bulk load: " + container.readAllItems(new PartitionKey("Family0"), Family.class).stream().count());
    }

    private void shutdown() {
        try {
            // To allow for the sequence to complete after subscribe() calls
            Thread.sleep(5000);
            //Clean shutdown
            logger.info("Deleting Cosmos DB resources");
            logger.info("-Deleting container...");
            if (container != null) container.delete();
            logger.info("-Closing the client...");
        } catch (InterruptedException err) {
            err.printStackTrace();
        } catch (Exception err) {
            logger.error("Deleting Cosmos DB resources failed, will still attempt to close the client. See stack " + "trace below.");
            err.printStackTrace();
        }
        client.close();
        logger.info("Done.");
    }
}
