// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.bulk.async;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.GlobalThroughputControlConfig;
import com.azure.cosmos.ThrottlingRetryOptions;
import com.azure.cosmos.ThroughputControlGroupConfig;
import com.azure.cosmos.ThroughputControlGroupConfigBuilder;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Families;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.models.CosmosBulkExecutionOptions;
import com.azure.cosmos.models.CosmosBulkItemResponse;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerRequestOptions;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosPatchOperations;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.identity.DefaultAzureCredentialBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SampleBulkHandleRetriesAsync {

    private static final Logger logger = LoggerFactory.getLogger(SampleBulkHandleRetriesAsync.class);
    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";
    private CosmosAsyncClient client;
    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;
    private int noOfItemsToCreate = 500;

    public static void main(String[] args) {
        SampleBulkHandleRetriesAsync p = new SampleBulkHandleRetriesAsync();

        try {
            logger.info("Starting ASYNC main");
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
            .consistencyLevel(ConsistencyLevel.SESSION).buildAsyncClient();

 
        createDatabaseIfNotExists();
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

    private void createDatabaseIfNotExists() {
        logger.info("Create database " + databaseName + " if not exists.");

        //  Create database if not exists
        //  <CreateDatabaseIfNotExists>
        Mono<CosmosDatabaseResponse> databaseIfNotExists = client.createDatabaseIfNotExists(databaseName);
        databaseIfNotExists.flatMap(databaseResponse -> {
            database = client.getDatabase(databaseResponse.getProperties().getId());
            logger.info("Checking database " + database.getId() + " completed!\n");
            return Mono.empty();
        }).block();
        //  </CreateDatabaseIfNotExists>
    }

    private void createContainerIfNotExists() {
        logger.info("Create container " + containerName + " if not exists.");

        //  Create container if not exists
        //  <CreateContainerIfNotExists>

        CosmosContainerProperties containerProperties = new CosmosContainerProperties(
            containerName, "/lastName");
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);
        Mono<CosmosContainerResponse> containerIfNotExists = database
            .createContainerIfNotExists(containerProperties, throughputProperties);

        //  Create container with 400 RU/s
        CosmosContainerResponse cosmosContainerResponse = containerIfNotExists.block();
        assert(cosmosContainerResponse != null);
        assert(cosmosContainerResponse.getProperties() != null);
        container = database.getContainer(cosmosContainerResponse.getProperties().getId());
        //  </CreateContainerIfNotExists>

        //Modify existing container
        containerProperties = cosmosContainerResponse.getProperties();
        Mono<CosmosContainerResponse> propertiesReplace =
            container.replace(containerProperties, new CosmosContainerRequestOptions());
        propertiesReplace.flatMap(containerResponse -> {
            logger.info(
                "setupContainer(): Container {}} in {} has been updated with it's new properties.",
                container.getId(),
                database.getId());
            return Mono.empty();
        }).onErrorResume((exception) -> {
            logger.error(
                "setupContainer(): Unable to update properties for container {} in database {}. e: {}",
                container.getId(),
                database.getId(),
                exception.getLocalizedMessage(),
                exception);
            return Mono.empty();
        }).block();

    }

    private void largeBulkUpsertItemsWithBulkWriterAbstraction(Iterable<Family> families) {
        List<CosmosItemOperation> cosmosItemOperations = new ArrayList<>();
        for (Family family : families) {
            cosmosItemOperations.add(CosmosBulkOperations.getUpsertItemOperation(family, new PartitionKey(family.getLastName())));
        }
        BulkWriter bulkWriter = new BulkWriter(container);
        for (CosmosItemOperation operation : cosmosItemOperations) {
            bulkWriter.scheduleWrites(operation);
        }
        bulkWriter.execute().subscribe();
        //get count of items in container
        try {
            logger.info("Waiting for bulk operations to complete...");
            Thread.sleep(20000); // Wait for the bulk operations to complete
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        logger.info("Number of items to create was: " + noOfItemsToCreate);
        logger.info("Total items created after bulk load: " + container.readAllItems(new PartitionKey("Family0"), Family.class).count().block());
    }
    

    private void shutdown() {
        try {
            // To allow for the sequence to complete after subscribe() calls
            Thread.sleep(5000);
            //Clean shutdown
            logger.info("Deleting Cosmos DB resources");
            logger.info("-Deleting container...");
            if (container != null) container.delete().subscribe();
            logger.info("-Deleting database...");
            if (database != null) database.delete().subscribe();
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
