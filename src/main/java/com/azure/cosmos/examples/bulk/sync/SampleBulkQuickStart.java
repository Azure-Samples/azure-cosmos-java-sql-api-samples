// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.bulk.sync;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.ThroughputControlGroupConfig;
import com.azure.cosmos.ThroughputControlGroupConfigBuilder;
import com.azure.cosmos.GlobalThroughputControlConfig;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Families;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosPatchOperations;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.models.CosmosContainerRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.CosmosBulkOperationResponse;
import com.azure.cosmos.models.CosmosBulkExecutionOptions;
import com.azure.cosmos.models.CosmosBulkItemResponse;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SampleBulkQuickStart {

    private static final Logger logger = LoggerFactory.getLogger(SampleBulkQuickStart.class);
    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";
    private CosmosClient client;
    private CosmosDatabase database;
    private CosmosContainer container;

    public static void main(String[] args) {
        SampleBulkQuickStart p = new SampleBulkQuickStart();

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
                .consistencyLevel(ConsistencyLevel.SESSION).buildClient();

        //  </CreateAsyncClient>

        createDatabaseIfNotExists();
        createContainerIfNotExists();

        //  <CreateDocs>
        Family andersenFamilyItem = Families.getAndersenFamilyItem();
        Family wakefieldFamilyItem = Families.getWakefieldFamilyItem();
        Family johnsonFamilyItem = Families.getJohnsonFamilyItem();
        Family smithFamilyItem = Families.getSmithFamilyItem();

        ArrayList<Family> families = new ArrayList<Family>();
        families.add(andersenFamilyItem);
        families.add(wakefieldFamilyItem);
        families.add(johnsonFamilyItem);
        families.add(smithFamilyItem);
        //  </CreateDocs>

        logger.info("Bulk creates.");
        bulkCreateItems(families);
        bulkCreateItems(families);

        andersenFamilyItem.setRegistered(false);
        wakefieldFamilyItem.setRegistered(false);

        ArrayList<Family> familiesToUpsert = new ArrayList<>();
        familiesToUpsert.add(andersenFamilyItem);
        familiesToUpsert.add(wakefieldFamilyItem);
        familiesToUpsert.add(johnsonFamilyItem);
        familiesToUpsert.add(smithFamilyItem);
        logger.info("Bulk upserts.");
        bulkUpsertItems(familiesToUpsert);

        andersenFamilyItem.setRegistered(true);
        wakefieldFamilyItem.setRegistered(true);

        ArrayList<Family> familiesToReplace = new ArrayList<Family>();
        familiesToReplace.add(andersenFamilyItem);
        familiesToReplace.add(wakefieldFamilyItem);
        familiesToReplace.add(johnsonFamilyItem);
        familiesToReplace.add(smithFamilyItem);
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
        CosmosDatabaseResponse databaseIfNotExists = client.createDatabaseIfNotExists(databaseName);
        database = client.getDatabase(databaseIfNotExists.getProperties().getId());
        logger.info("Checking database " + database.getId() + " completed!\n");
        //  </CreateDatabaseIfNotExists>
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


    //  <BulkCreateItems>
    private void bulkCreateItems(Iterable<Family> families) {
        List<CosmosItemOperation> cosmosItemOperations = new ArrayList<CosmosItemOperation>();
        for (Family family : families) {
            cosmosItemOperations.add(CosmosBulkOperations.getCreateItemOperation(family, new PartitionKey(family.getLastName())));
        }
        container.executeBulkOperations(cosmosItemOperations);
    }
    //  </BulkCreateItems>

    //  <BulkDeleteItems>
    private void bulkDeleteItems(Iterable<Family> families) {
        List<CosmosItemOperation> cosmosItemOperations = new ArrayList<CosmosItemOperation>();
        for (Family family : families) {
            cosmosItemOperations.add(CosmosBulkOperations.getDeleteItemOperation(family.getId(), new PartitionKey(family.getLastName())));
        }
        container.executeBulkOperations(cosmosItemOperations);
    }
    //  </BulkDeleteItems>

    //  <BulkUpsertItems>
    private void bulkUpsertItems(Iterable<Family> families) {
        List<CosmosItemOperation> cosmosItemOperations = new ArrayList<CosmosItemOperation>();
        for (Family family : families) {
            cosmosItemOperations.add(CosmosBulkOperations.getUpsertItemOperation(family, new PartitionKey(family.getLastName())));
        }
        container.executeBulkOperations(cosmosItemOperations);
    }
    //  </BulkUpsertItems>

    //  <BulkPatchItems>
    private void bulkPatchItems(Iterable<Family> families, CosmosPatchOperations operations) {
        List<CosmosItemOperation> cosmosItemOperations = new ArrayList<CosmosItemOperation>();
        for (Family family : families) {
            cosmosItemOperations.add(CosmosBulkOperations.getPatchItemOperation(family.getId(), new PartitionKey(family.getLastName()), operations));
        }
        container.executeBulkOperations(cosmosItemOperations);
    }
    //  </BulkPatchItems>

    //  <BulkReplaceItems>
    private void bulkReplaceItems(Iterable<Family> families) {
        List<CosmosItemOperation> cosmosItemOperations = new ArrayList<CosmosItemOperation>();
        for (Family family : families) {
            cosmosItemOperations.add(CosmosBulkOperations.getReplaceItemOperation(family.getId(), family, new PartitionKey(family.getLastName())));
        }
        container.executeBulkOperations(cosmosItemOperations);
    }
    //  </BulkReplaceItems>

    //  <BulkCreateItemsWithResponseProcessingAndExecutionOptions>
    private void bulkCreateItemsWithResponseProcessing(Iterable<Family> families) {


        List<CosmosItemOperation> cosmosItemOperations = new ArrayList<CosmosItemOperation>();
        for (Family family : families) {
            cosmosItemOperations.add(CosmosBulkOperations.getCreateItemOperation(family, new PartitionKey(family.getLastName())));
        }

        Iterable<CosmosBulkOperationResponse<Object>> cosmosBulkOperationResponse = container.executeBulkOperations(cosmosItemOperations);
        for (CosmosBulkOperationResponse<Object> response : cosmosBulkOperationResponse) {
            CosmosBulkItemResponse cosmosBulkItemResponse;
            cosmosBulkItemResponse = response.getResponse();
            CosmosItemOperation cosmosItemOperation;
            cosmosItemOperation = response.getOperation();

            if (response.getException() != null) {
                logger.error("Bulk operation failed", response.getException());
            } else if (cosmosBulkItemResponse == null ||
                    !response.getResponse().isSuccessStatusCode()) {

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
                throw new IllegalStateException("No response retrieved.");
            }

        }

    }

    private void bulkCreateItemsWithBulkWriterAbstractionAndGlobalThroughputControl() {
        String controlContainerId = "throughputControlContainer";
        CosmosContainer controlContainer = database.getContainer(controlContainerId);
        database.createContainerIfNotExists(controlContainer.getId(), "/groupId");

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
        com.azure.cosmos.examples.bulk.sync.BulkWriter bulkWriter = new BulkWriter(container);
        bulkWriter.scheduleWrites(andersonItemOperation);
        bulkWriter.scheduleWrites(wakeFieldItemOperation);
        bulkWriter.execute();
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
        com.azure.cosmos.examples.bulk.sync.BulkWriter bulkWriter = new com.azure.cosmos.examples.bulk.sync.BulkWriter(container);
        bulkWriter.scheduleWrites(andersonItemOperation);
        bulkWriter.scheduleWrites(wakeFieldItemOperation);
        bulkWriter.execute();
    }

    private void bulkUpsertItemsWithBulkWriterAbstraction() {
        Family andersenFamilyItem = Families.getAndersenFamilyItem();
        Family wakefieldFamilyItem = Families.getWakefieldFamilyItem();
        CosmosItemOperation wakeFieldItemOperation = CosmosBulkOperations.getUpsertItemOperation(wakefieldFamilyItem, new PartitionKey(wakefieldFamilyItem.getLastName()));
        CosmosItemOperation andersonItemOperation = CosmosBulkOperations.getUpsertItemOperation(andersenFamilyItem, new PartitionKey(andersenFamilyItem.getLastName()));
        com.azure.cosmos.examples.bulk.sync.BulkWriter bulkWriter = new com.azure.cosmos.examples.bulk.sync.BulkWriter(container);
        bulkWriter.scheduleWrites(andersonItemOperation);
        bulkWriter.scheduleWrites(wakeFieldItemOperation);
        bulkWriter.execute();
    }


    private void bulkCreateItemsWithExecutionOptions(Iterable<Family> families) {


        CosmosBulkExecutionOptions bulkExecutionOptions = new CosmosBulkExecutionOptions();

        // The default value for maxMicroBatchConcurrency is 1.
        // By increasing it, it means more concurrent requests will be allowed to be sent to the server, which leads to increased RU usage.
        //
        // Before you increase the value, please examine the RU usage of your container - whether it has been saturated or not.
        // When the RU has already been under saturation, increasing the concurrency will not help the situation,
        // rather it may cause more 429 and request timeout.
        bulkExecutionOptions.setMaxMicroBatchConcurrency(2);
        List<CosmosItemOperation> cosmosItemOperations = new ArrayList<CosmosItemOperation>();
        for (Family family : families) {
            cosmosItemOperations.add(CosmosBulkOperations.getCreateItemOperation(family, new PartitionKey(family.getLastName())));
        }
        container.executeBulkOperations(cosmosItemOperations, bulkExecutionOptions);
    }

    private void shutdown() {
        try {
            // To allow for the sequence to complete after subscribe() calls
            Thread.sleep(5000);
            //Clean shutdown
            logger.info("Deleting Cosmos DB resources");
            logger.info("-Deleting container...");
            if (container != null) container.delete();
            logger.info("-Deleting database...");
            if (database != null) database.delete();
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
