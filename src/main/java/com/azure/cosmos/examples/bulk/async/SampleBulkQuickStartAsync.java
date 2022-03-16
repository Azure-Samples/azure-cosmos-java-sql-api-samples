// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.bulk.async;

import com.azure.cosmos.*;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Families;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.implementation.ImplementationBridgeHelpers;
import com.azure.cosmos.models.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;


public class SampleBulkQuickStartAsync {

    private static Logger logger = LoggerFactory.getLogger(SampleBulkQuickStartAsync.class);
    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";
    private CosmosAsyncClient client;
    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    public static void main(String[] args) {
        SampleBulkQuickStartAsync p = new SampleBulkQuickStartAsync();

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

    private void getStartedDemo() throws Exception {

        logger.info("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        ArrayList<String> preferredRegions = new ArrayList<String>();
        preferredRegions.add("West US");

        //  Setting the preferred location to Cosmos DB Account region
        //  West US is just an example. User should set preferred location to the Cosmos DB region closest to the
        //  application

        //  Create async client
        //  <CreateAsyncClient>
        client = new CosmosClientBuilder().endpoint(AccountSettings.HOST).key(AccountSettings.MASTER_KEY).preferredRegions(preferredRegions).contentResponseOnWriteEnabled(true).consistencyLevel(ConsistencyLevel.SESSION).buildAsyncClient();

        //  </CreateAsyncClient>

        createDatabaseIfNotExists();
        createContainerIfNotExists();

        Family andersenFamilyItem = Families.getAndersenFamilyItem();
        Family wakefieldFamilyItem = Families.getWakefieldFamilyItem();
        Family johnsonFamilyItem = Families.getJohnsonFamilyItem();
        Family smithFamilyItem = Families.getSmithFamilyItem();

        //  Setup family items to create
        Flux<Family> families = Flux.just(andersenFamilyItem, wakefieldFamilyItem, johnsonFamilyItem, smithFamilyItem);

        logger.info("Bulk creates.");
        bulkCreateItems(families);
        bulkCreateItems(families);

        andersenFamilyItem.setRegistered(false);
        wakefieldFamilyItem.setRegistered(false);

        Flux<Family> familiesToUpsert = Flux.just(andersenFamilyItem, wakefieldFamilyItem, johnsonFamilyItem, smithFamilyItem);
        logger.info("Bulk upserts.");
        bulkUpsertItems(familiesToUpsert);

        andersenFamilyItem.setRegistered(true);
        wakefieldFamilyItem.setRegistered(true);

        Flux<Family> familiesToReplace = Flux.just(andersenFamilyItem, wakefieldFamilyItem, johnsonFamilyItem, smithFamilyItem);
        logger.info("Bulk replace.");
        bulkReplaceItems(familiesToReplace);
        logger.info("Bulk deletes.");
        bulkDeleteItems(families);
        logger.info("Bulk create response processing without errors.");
        bulkCreateItemsWithResponseProcessing(families);
        logger.info("Bulk create response processing with 409 error");
        bulkCreateItemsWithResponseProcessing(families);
        logger.info("Bulk deletes.");
        bulkDeleteItems(families);
        logger.info("Bulk creates with execution options.");
        bulkCreateItemsWithExecutionOptions(families);
        logger.info("Bulk deletes.");
        bulkDeleteItems(families);
        bulkCreateItemsSimple();
    }

    private void createDatabaseIfNotExists() throws Exception {
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

    private void createContainerIfNotExists() throws Exception {
        logger.info("Create container " + containerName + " if not exists.");

        //  Create container if not exists
        //  <CreateContainerIfNotExists>

        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, "/lastName");
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);
        Mono<CosmosContainerResponse> containerIfNotExists = database.createContainerIfNotExists(containerProperties, throughputProperties);

        //  Create container with 400 RU/s
        CosmosContainerResponse cosmosContainerResponse = containerIfNotExists.block();
        container = database.getContainer(cosmosContainerResponse.getProperties().getId());
        //  </CreateContainerIfNotExists>

        //Modify existing container
        containerProperties = cosmosContainerResponse.getProperties();
        Mono<CosmosContainerResponse> propertiesReplace = container.replace(containerProperties, new CosmosContainerRequestOptions());
        propertiesReplace.flatMap(containerResponse -> {
            logger.info("setupContainer(): Container " + container.getId() + " in " + database.getId() + "has been " + "updated with it's new " + "properties.");
            return Mono.empty();
        }).onErrorResume((exception) -> {
            logger.error("setupContainer(): Unable to update properties for container " + container.getId() + " in " + "database " + database.getId() + ". e: " + exception.getLocalizedMessage());
            return Mono.empty();
        }).block();

    }

    private void bulkCreateItems(Flux<Family> families) {
        Flux<CosmosItemOperation> cosmosItemOperations = families.map(family -> CosmosBulkOperations.getCreateItemOperation(family, new PartitionKey(family.getLastName())));
        container.executeBulkOperations(cosmosItemOperations).blockLast();
    }

    private void bulkDeleteItems(Flux<Family> families) {
        Flux<CosmosItemOperation> cosmosItemOperations = families.map(family -> CosmosBulkOperations.getDeleteItemOperation(family.getId(), new PartitionKey(family.getLastName())));
        container.executeBulkOperations(cosmosItemOperations).blockLast();
    }

    private void bulkUpsertItems(Flux<Family> families) {
        Flux<CosmosItemOperation> cosmosItemOperations = families.map(family -> CosmosBulkOperations.getUpsertItemOperation(family, new PartitionKey(family.getLastName())));
        container.executeBulkOperations(cosmosItemOperations).blockLast();
    }

    private void bulkReplaceItems(Flux<Family> families) {
        Flux<CosmosItemOperation> cosmosItemOperations = families.map(family -> CosmosBulkOperations.getReplaceItemOperation(family.getId(), family, new PartitionKey(family.getLastName())));
        container.executeBulkOperations(cosmosItemOperations).blockLast();
    }

    private void bulkCreateItemsWithResponseProcessing(Flux<Family> families) {
        Flux<CosmosItemOperation> cosmosItemOperations = families.map(family -> CosmosBulkOperations.getCreateItemOperation(family, new PartitionKey(family.getLastName())));
        container.executeBulkOperations(cosmosItemOperations).flatMap(cosmosBulkOperationResponse -> {
            CosmosBulkItemResponse cosmosBulkItemResponse = cosmosBulkOperationResponse.getResponse();
            CosmosItemOperation cosmosItemOperation = cosmosBulkOperationResponse.getOperation();

            if (cosmosBulkOperationResponse.getException() != null) {
                logger.error("Bulk operation failed", cosmosBulkOperationResponse.getException());
            } else if (cosmosBulkOperationResponse.getResponse() == null || !cosmosBulkOperationResponse.getResponse().isSuccessStatusCode()) {
                logger.error("The operation for Item ID: [{}]  Item PartitionKey Value: [{}] did not complete successfully with " + "a" + " {} response code.", cosmosItemOperation.<Family>getItem().getId(), cosmosItemOperation.<Family>getItem().getLastName(), cosmosBulkItemResponse.getStatusCode());
            } else {
                logger.info("Item ID: [{}]  Item PartitionKey Value: [{}]", cosmosItemOperation.<Family>getItem().getId(), cosmosItemOperation.<Family>getItem().getLastName());
                logger.info("Status Code: {}", String.valueOf(cosmosBulkItemResponse.getStatusCode()));
                logger.info("Request Charge: {}", String.valueOf(cosmosBulkItemResponse.getRequestCharge()));
            }
            return Mono.just(cosmosBulkItemResponse);
        }).blockLast();
    }

    private void bulkCreateItemsWithExecutionOptions(Flux<Family> families) {
        CosmosBulkExecutionOptions bulkExecutionOptions = new CosmosBulkExecutionOptions();
        ImplementationBridgeHelpers.CosmosBulkExecutionOptionsHelper.getCosmosBulkExecutionOptionsAccessor().setMaxMicroBatchSize(bulkExecutionOptions, 10);
        Flux<CosmosItemOperation> cosmosItemOperations = families.map(family -> CosmosBulkOperations.getCreateItemOperation(family, new PartitionKey(family.getLastName())));
        container.executeBulkOperations(cosmosItemOperations, bulkExecutionOptions).blockLast();
    }


    private void bulkCreateItemsSimple() {
        Family andersenFamilyItem = Families.getAndersenFamilyItem();
        Family wakefieldFamilyItem = Families.getWakefieldFamilyItem();
        CosmosItemOperation andersonItemOperation = CosmosBulkOperations.getCreateItemOperation(andersenFamilyItem, new PartitionKey(andersenFamilyItem.getLastName()));
        CosmosItemOperation wakeFieldItemOperation = CosmosBulkOperations.getCreateItemOperation(wakefieldFamilyItem, new PartitionKey(wakefieldFamilyItem.getLastName()));
        BulkWriter bulkWriter = new BulkWriter(container);
        bulkWriter.scheduleWrites(andersonItemOperation);
        bulkWriter.scheduleWrites(wakeFieldItemOperation);
        bulkWriter.execute().blockLast();
    }

    private void shutdown() {
        try {
            //Clean shutdown
            logger.info("Deleting Cosmos DB resources");
            logger.info("-Deleting container...");
            if (container != null) container.delete().subscribe();
            logger.info("-Deleting database...");
            if (database != null) database.delete().subscribe();
            logger.info("-Closing the client...");
        } catch (Exception err) {
            logger.error("Deleting Cosmos DB resources failed, will still attempt to close the client. See stack " + "trace below.");
            err.printStackTrace();
        }
        client.close();
        logger.info("Done.");
    }


}
