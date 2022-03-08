// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.batch.async;

import com.azure.cosmos.*;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Families;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.models.*;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.ArrayList;

public class SampleBatchQuickStartAsync {
    private static Logger logger = LoggerFactory.getLogger(SampleBatchQuickStartAsync.class);
    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";
    private CosmosAsyncClient client;
    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    public static void main(String[] args) {
        SampleBatchQuickStartAsync p = new SampleBatchQuickStartAsync();

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
        //  West US is just an example. User should set preferred location to the Cosmos DB region closest to the application

        //  Create async client
        //  <CreateAsyncClient>
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .preferredRegions(preferredRegions)
                .contentResponseOnWriteEnabled(true)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .buildAsyncClient();
        //  </CreateAsyncClient>
        Family andersenFamily = Families.getAndersenFamilyItem();
        createDatabaseIfNotExists();
        createContainerIfNotExists();

        logger.info("A batch of create operations that succeed.");
        Family anotherAndersenFamily = Families.getAndersenFamilyItem();
        CosmosBatch batchOfCreateOperations = CosmosBatch.createCosmosBatch(new PartitionKey(andersenFamily.getLastName()));
        //creating two create operations for distinct family items
        batchOfCreateOperations.createItemOperation(andersenFamily);
        batchOfCreateOperations.createItemOperation(anotherAndersenFamily);
        //executing the batch of operations
        executeBatchOperations(batchOfCreateOperations);
        deleteItem(andersenFamily);
        deleteItem(anotherAndersenFamily);

        logger.info("A batch of create operations that fails.");
        CosmosBatch batchOfCreateOperationsThatFail = CosmosBatch.createCosmosBatch(new PartitionKey(andersenFamily.getLastName()));
        //creating three create operations with the same family item
        //This batch will fail because an attempt is made to create the same family item a second time.
        batchOfCreateOperationsThatFail.createItemOperation(andersenFamily);
        batchOfCreateOperationsThatFail.createItemOperation(andersenFamily);
        batchOfCreateOperationsThatFail.createItemOperation(andersenFamily);
        //executing the batch of operations
        executeBatchOperations(batchOfCreateOperationsThatFail);

        logger.info("A batch of create and replace operations that succeeds.");
        CosmosBatch batchOfCreateAndReplaceOperations = CosmosBatch.createCosmosBatch(new PartitionKey(andersenFamily.getLastName()));
        //creating a create operation and a replace operation for the same family item
        batchOfCreateAndReplaceOperations.createItemOperation(andersenFamily);
        andersenFamily.setRegistered(false);
        batchOfCreateAndReplaceOperations.replaceItemOperation(andersenFamily.getId(), andersenFamily);
        //executing the batch of operations
        executeBatchOperations(batchOfCreateAndReplaceOperations);
        deleteItem(andersenFamily);

        logger.info("A batch of create and replace operations that fails.");
        CosmosBatch batchOfCreateAndReplaceOperationsThatFail = CosmosBatch.createCosmosBatch(new PartitionKey(andersenFamily.getLastName()));
        //creating a create operation and a replace operation with two distinct families.
        //This batch will fail because an attempt is made to replace a non-existent family item
        batchOfCreateAndReplaceOperationsThatFail.createItemOperation(andersenFamily);
        anotherAndersenFamily = Families.getAndersenFamilyItem();
        batchOfCreateAndReplaceOperationsThatFail.replaceItemOperation(anotherAndersenFamily.getId(), anotherAndersenFamily);
        //executing the batch of operations
        executeBatchOperations(batchOfCreateAndReplaceOperationsThatFail);

        logger.info("A batch of operations containing a read operation.");
        CosmosBatch batchWithReadOperation = CosmosBatch.createCosmosBatch(new PartitionKey(andersenFamily.getLastName()));
        //creating a create operation, a replace operation for the same family item and then reading the same family item
        batchWithReadOperation.createItemOperation(andersenFamily);
        andersenFamily.setRegistered(true);
        batchWithReadOperation.replaceItemOperation(andersenFamily.getId(), andersenFamily);
        //read operation
        batchWithReadOperation.readItemOperation(andersenFamily.getId());
        //executing the batch of operations
        executeBatchOperationsWithRead(batchWithReadOperation);
        deleteItem(andersenFamily);

        logger.info("A batch of upsert and delete operations.");
        CosmosBatch batchOfUpsertAndDeleteOperations = CosmosBatch.createCosmosBatch(new PartitionKey(andersenFamily.getLastName()));
        //creating an upsert operation and a delete operation for the same family item
        batchOfUpsertAndDeleteOperations.upsertItemOperation(andersenFamily);
        batchOfUpsertAndDeleteOperations.deleteItemOperation(andersenFamily.getId());
        //executing the batch of operations
        executeBatchOperations(batchOfUpsertAndDeleteOperations);
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
        Mono<CosmosContainerResponse> containerIfNotExists = database.createContainerIfNotExists(containerProperties,
                throughputProperties);

        //  Create container with 400 RU/s
        CosmosContainerResponse cosmosContainerResponse = containerIfNotExists.block();
        container = database.getContainer(cosmosContainerResponse.getProperties().getId());
        //  </CreateContainerIfNotExists>

        //Modify existing container
        containerProperties = cosmosContainerResponse.getProperties();
        Mono<CosmosContainerResponse> propertiesReplace = container.replace(containerProperties,
                new CosmosContainerRequestOptions());
        propertiesReplace.flatMap(containerResponse -> {
            logger.info("setupContainer(): Container " + container.getId() + " in " +
                    database.getId() + "has been " + "updated with it's new " + "properties.");
            return Mono.empty();
        }).onErrorResume((exception) -> {
            logger.error("setupContainer(): Unable to update properties for container " + container.getId()
                    + " in " + "database " + database.getId() + ". e: " + exception.getLocalizedMessage());
            return Mono.empty();
        }).block();

    }

    private void executeBatchOperations(CosmosBatch batch) {
        container.executeCosmosBatch(batch).map(cosmosBatchResponse -> {
            //Examining if the batch of operations is successful
            if (cosmosBatchResponse.isSuccessStatusCode()) {
                logger.info("The batch of operations succeeded.");
            } else {
                //Iterating over the operation results to find out the error code
                cosmosBatchResponse.getResults().stream().forEach(operationResult -> {
                    //Failed operation will have a status code of the corresponding error.
                    //All other operations will have a 424 (Failed Dependency) status code.
                    if (operationResult.getStatusCode() != HttpResponseStatus.FAILED_DEPENDENCY.code()) {
                        CosmosItemOperation itemOperation = operationResult.getOperation();
                        logger.info("Operation for Item with ID [{}] and Partition Key Value [{}]" +
                                        " failed with a status code [{}], resulting in batch failure.",
                                itemOperation.<Family>getItem().getId(),
                                itemOperation.<Family>getItem().getLastName(),
                                operationResult.getStatusCode());
                    }
                });
            }
            return cosmosBatchResponse;
        }).block();
    }

    private void executeBatchOperationsWithRead(CosmosBatch batch) {
        container.executeCosmosBatch(batch).map(cosmosBatchResponse -> {
            //Examining if the batch of operations is successful
            if (cosmosBatchResponse.isSuccessStatusCode()) {
                logger.info("The batch of operations succeeded.");
                Family family = cosmosBatchResponse.getResults().get(2).getItem(Family.class);
                logger.info("Read operation returned a family with ID [{}] and Partition Key Value [{}]", family.getId(), family.getLastName());
            } else {
                //Iterating over the operation results to find out the error code
                cosmosBatchResponse.getResults().stream().forEach(operationResult -> {
                    //Failed operation will have a status code of the corresponding error.
                    //All other operations will have a 424 (Failed Dependency) status code.
                    if (operationResult.getStatusCode() != HttpResponseStatus.FAILED_DEPENDENCY.code()) {
                        CosmosItemOperation itemOperation = operationResult.getOperation();
                        logger.info("Operation for Item with ID [{}] and Partition Key Value [{}]" +
                                        " failed with a status code [{}], resulting in batch failure.",
                                itemOperation.<Family>getItem().getId(),
                                itemOperation.<Family>getItem().getLastName(),
                                operationResult.getStatusCode());
                    }
                });
            }
            return cosmosBatchResponse;
        }).block();
    }


    private void deleteItem(Family item) {
        container.deleteItem(item.getId(), new PartitionKey(item.getLastName())).block();
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
            logger.error("Deleting Cosmos DB resources failed, will still attempt to close the client. See stack " +
                    "trace below.");
            err.printStackTrace();
        }
        client.close();
        logger.info("Done.");
        //Clean shutdown
    }

}
