// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.bulk.async;

import com.azure.cosmos.*;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Families;
import com.azure.cosmos.examples.common.Family;
//  <CosmosBulkOperationsImport>
import com.azure.cosmos.models.*;
//  </CosmosBulkOperationsImport>
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;

public class SampleBulkQuickStartAsync {

    private static final Logger logger = LoggerFactory.getLogger(SampleBulkQuickStartAsync.class);
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

    private void getStartedDemo() {

        logger.info("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        ArrayList<String> preferredRegions = new ArrayList<>();
        preferredRegions.add("West US");

        //  Setting the preferred location to Cosmos DB Account region
        //  West US is just an example. User should set preferred location to the Cosmos DB region closest to the
        //  application

        //  Create async client
        //  <CreateAsyncClient>
        client = new CosmosClientBuilder()
            .endpoint(AccountSettings.HOST)
            .key(AccountSettings.MASTER_KEY)
            .preferredRegions(preferredRegions)
            .contentResponseOnWriteEnabled(true)
            .consistencyLevel(ConsistencyLevel.SESSION).buildAsyncClient();

        //  </CreateAsyncClient>
 
        createDatabaseIfNotExists();
        createContainerIfNotExists();

        //  <AddDocsToStream>
        Family andersenFamilyItem = Families.getAndersenFamilyItem();
        Family wakefieldFamilyItem = Families.getWakefieldFamilyItem();
        Family johnsonFamilyItem = Families.getJohnsonFamilyItem();
        Family smithFamilyItem = Families.getSmithFamilyItem();

        //  Setup family items to create
        Flux<Family> families = Flux.just(andersenFamilyItem, wakefieldFamilyItem, johnsonFamilyItem, smithFamilyItem);
        //  </AddDocsToStream>

        logger.info("Bulk creates.");
        bulkCreateItems(families);
        bulkCreateItems(families);

        andersenFamilyItem.setRegistered(false);
        wakefieldFamilyItem.setRegistered(false);

        Flux<Family> familiesToUpsert = Flux.just(
            andersenFamilyItem, wakefieldFamilyItem, johnsonFamilyItem, smithFamilyItem);
        logger.info("Bulk upserts.");
        bulkUpsertItems(familiesToUpsert);

        andersenFamilyItem.setRegistered(true);
        wakefieldFamilyItem.setRegistered(true);

        Flux<Family> familiesToReplace = Flux.just(
            andersenFamilyItem, wakefieldFamilyItem, johnsonFamilyItem, smithFamilyItem);
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
        logger.info("Bulk patches.");
        //  <PatchOperations>
        CosmosPatchOperations patchOps = CosmosPatchOperations.create().add("/country", "United States")
                .set("/registered", 0);
        //  </PatchOperations>
        // Note: here we apply "add" and "set" to patch elements whose root parent
        // exists, but we cannot do this where the root parent does not exist. When this
        // is required, read the full document first, then use replace.                
        bulkPatchItems(familiesToReplace, patchOps);        
        logger.info("Bulk deletes.");
        bulkDeleteItems(families);
        logger.info("Bulk upserts with BulkWriter abstraction");
        bulkUpsertItemsWithBulkWriterAbstraction();
        logger.info("Bulk upserts with BulkWriter Abstraction and Local Throughput Control");
        bulkUpsertItemsWithBulkWriterAbstractionAndLocalThroughPutControl();
        logger.info("Bulk upserts with BulkWriter Abstraction and Global Throughput Control");
        bulkCreateItemsWithBulkWriterAbstractionAndGlobalThroughputControl();
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

    
    //  <BulkCreateItems>
    private void bulkCreateItems(Flux<Family> families) {
        Flux<CosmosItemOperation> cosmosItemOperations = families.map(
            family -> CosmosBulkOperations.getCreateItemOperation(family, new PartitionKey(family.getLastName())));
        container.executeBulkOperations(cosmosItemOperations).blockLast();
    }
    //  </BulkCreateItems>

    //  <BulkDeleteItems>
    private void bulkDeleteItems(Flux<Family> families) {
        Flux<CosmosItemOperation> cosmosItemOperations = families.map(
            family -> CosmosBulkOperations
                .getDeleteItemOperation(family.getId(), new PartitionKey(family.getLastName())));
        container.executeBulkOperations(cosmosItemOperations).blockLast();
    }
    //  </BulkDeleteItems>

    //  <BulkUpsertItems>
    private void bulkUpsertItems(Flux<Family> families) {
        Flux<CosmosItemOperation> cosmosItemOperations = families.map(
            family -> CosmosBulkOperations.getUpsertItemOperation(family, new PartitionKey(family.getLastName())));
        container.executeBulkOperations(cosmosItemOperations).blockLast();
    }
    //  </BulkUpsertItems>   

    //  <BulkPatchItems>
    private void bulkPatchItems(Flux<Family> families, CosmosPatchOperations operations) {
        Flux<CosmosItemOperation> cosmosItemOperations = families.map(
            family -> CosmosBulkOperations
                .getPatchItemOperation(family.getId(), new PartitionKey(family.getLastName()), operations));
        container.executeBulkOperations(cosmosItemOperations).blockLast();
    }
    //  </BulkPatchItems>

    //  <BulkReplaceItems>
    private void bulkReplaceItems(Flux<Family> families) {
        Flux<CosmosItemOperation> cosmosItemOperations = families.map(
            family -> CosmosBulkOperations
                .getReplaceItemOperation(family.getId(), family, new PartitionKey(family.getLastName())));
        container.executeBulkOperations(cosmosItemOperations).blockLast();
    }
    //  </BulkReplaceItems>

    //  <BulkCreateItemsWithResponseProcessingAndExecutionOptions>
    private void bulkCreateItemsWithResponseProcessing(Flux<Family> families) {
        Flux<CosmosItemOperation> cosmosItemOperations = families.map(
            family -> CosmosBulkOperations.getCreateItemOperation(family, new PartitionKey(family.getLastName())));
        container.executeBulkOperations(cosmosItemOperations).flatMap(cosmosBulkOperationResponse -> {

            CosmosBulkItemResponse cosmosBulkItemResponse = cosmosBulkOperationResponse.getResponse();
            CosmosItemOperation cosmosItemOperation = cosmosBulkOperationResponse.getOperation();

            if (cosmosBulkOperationResponse.getException() != null) {
                logger.error("Bulk operation failed", cosmosBulkOperationResponse.getException());
            } else if (cosmosBulkItemResponse == null ||
                !cosmosBulkOperationResponse.getResponse().isSuccessStatusCode()) {

                logger.error(
                    "The operation for Item ID: [{}]  Item PartitionKey Value: [{}] did not complete " +
                        "successfully with " + "a" + " {} response code.",
                    cosmosItemOperation.<Family>getItem().getId(),
                    cosmosItemOperation.<Family>getItem().getLastName(),
                    cosmosBulkItemResponse != null ? cosmosBulkItemResponse.getStatusCode() : "n/a");
            } else {
                logger.info(
                    "Item ID: [{}]  Item PartitionKey Value: [{}]",
                    cosmosItemOperation.<Family>getItem().getId(),
                    cosmosItemOperation.<Family>getItem().getLastName());
                logger.info("Status Code: {}", cosmosBulkItemResponse.getStatusCode());
                logger.info("Request Charge: {}", cosmosBulkItemResponse.getRequestCharge());
            }
            if (cosmosBulkItemResponse == null) {
                return Mono.error(new IllegalStateException("No response retrieved."));
            } else {
                return Mono.just(cosmosBulkItemResponse);
            }
        }).blockLast();
    }

    private void bulkCreateItemsWithExecutionOptions(Flux<Family> families) {
        CosmosBulkExecutionOptions bulkExecutionOptions = new CosmosBulkExecutionOptions();

        // The default value for maxMicroBatchConcurrency is 1.
        // By increasing it, it means more concurrent requests will be allowed to be sent to the server, which leads to increased RU usage.
        //
        // Before you increase the value, please examine the RU usage of your container - whether it has been saturated or not.
        // When the RU has already been under saturation, increasing the concurrency will not help the situation,
        // rather it may cause more 429 and request timeout.
        bulkExecutionOptions.setMaxMicroBatchConcurrency(2);
        Flux<CosmosItemOperation> cosmosItemOperations = families.map(family -> CosmosBulkOperations.getCreateItemOperation(family, new PartitionKey(family.getLastName())));
        container.executeBulkOperations(cosmosItemOperations, bulkExecutionOptions).blockLast();
    }
    //  </BulkCreateItemsWithResponseProcessingAndExecutionOptions>

    //  <BulkWriterAbstraction>
    private void bulkUpsertItemsWithBulkWriterAbstraction() {
        Family andersenFamilyItem = Families.getAndersenFamilyItem();
        Family wakefieldFamilyItem = Families.getWakefieldFamilyItem();
        CosmosItemOperation wakeFieldItemOperation = CosmosBulkOperations.getUpsertItemOperation(wakefieldFamilyItem, new PartitionKey(wakefieldFamilyItem.getLastName()));
        CosmosItemOperation andersonItemOperation = CosmosBulkOperations.getUpsertItemOperation(andersenFamilyItem, new PartitionKey(andersenFamilyItem.getLastName()));
        BulkWriter bulkWriter = new BulkWriter(container);
        bulkWriter.scheduleWrites(andersonItemOperation);
        bulkWriter.scheduleWrites(wakeFieldItemOperation);
        bulkWriter.execute().subscribe();
    }

    private void bulkUpsertItemsWithBulkWriterAbstractionAndLocalThroughPutControl() {
        ThroughputControlGroupConfig groupConfig =
                new ThroughputControlGroupConfigBuilder()
                        .setGroupName("group1")
                        .setTargetThroughput(200)
                        .build();
        container.enableLocalThroughputControlGroup(groupConfig);
        Family andersenFamilyItem = Families.getAndersenFamilyItem();
        Family wakefieldFamilyItem = Families.getWakefieldFamilyItem();
        CosmosItemOperation wakeFieldItemOperation = CosmosBulkOperations.getUpsertItemOperation(wakefieldFamilyItem, new PartitionKey(wakefieldFamilyItem.getLastName()));
        CosmosItemOperation andersonItemOperation = CosmosBulkOperations.getUpsertItemOperation(andersenFamilyItem, new PartitionKey(andersenFamilyItem.getLastName()));
        BulkWriter bulkWriter = new BulkWriter(container);
        bulkWriter.scheduleWrites(andersonItemOperation);
        bulkWriter.scheduleWrites(wakeFieldItemOperation);
        bulkWriter.execute().subscribe();
    }

    private void bulkCreateItemsWithBulkWriterAbstractionAndGlobalThroughputControl() {
        String controlContainerId = "throughputControlContainer";
        CosmosAsyncContainer controlContainer = database.getContainer(controlContainerId);
        database.createContainerIfNotExists(controlContainer.getId(), "/groupId").block();

        ThroughputControlGroupConfig groupConfig =
                new ThroughputControlGroupConfigBuilder()
                        .setGroupName("group-" + UUID.randomUUID())
                        .setTargetThroughput(200)
                        .build();

        GlobalThroughputControlConfig globalControlConfig = this.client.createGlobalThroughputControlConfigBuilder(this.database.getId(), controlContainerId)
                .setControlItemRenewInterval(Duration.ofSeconds(5))
                .setControlItemExpireInterval(Duration.ofSeconds(20))
                .build();

        container.enableGlobalThroughputControlGroup(groupConfig, globalControlConfig);
        CosmosItemRequestOptions requestOptions = new CosmosItemRequestOptions();
        requestOptions.setThroughputControlGroupName(groupConfig.getGroupName());
        Family andersenFamilyItem = Families.getAndersenFamilyItem();
        Family wakefieldFamilyItem = Families.getWakefieldFamilyItem();
        CosmosItemOperation andersonItemOperation = CosmosBulkOperations.getCreateItemOperation(andersenFamilyItem, new PartitionKey(andersenFamilyItem.getLastName()));
        CosmosItemOperation wakeFieldItemOperation = CosmosBulkOperations.getCreateItemOperation(wakefieldFamilyItem, new PartitionKey(wakefieldFamilyItem.getLastName()));
        BulkWriter bulkWriter = new BulkWriter(container);
        bulkWriter.scheduleWrites(andersonItemOperation);
        bulkWriter.scheduleWrites(wakeFieldItemOperation);
        bulkWriter.execute().subscribe();
    }
    //  </BulkWriterAbstraction>
    

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
