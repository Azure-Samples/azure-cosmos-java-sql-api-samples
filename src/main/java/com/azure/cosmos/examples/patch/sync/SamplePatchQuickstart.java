// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.patch.sync;

import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Families;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.examples.common.Child;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.models.CosmosBulkOperationResponse;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;

import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosPatchItemRequestOptions;
import com.azure.cosmos.models.CosmosPatchOperations;
import com.azure.cosmos.models.CosmosBatch;
import com.azure.cosmos.models.CosmosBatchOperationResult;
import com.azure.cosmos.models.CosmosBatchPatchItemRequestOptions;
import com.azure.cosmos.models.CosmosBatchResponse;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SamplePatchQuickstart {

    private CosmosClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";

    private CosmosDatabase database;
    private CosmosContainer container;

   private static Logger logger = LoggerFactory.getLogger(SamplePatchQuickstart.class);

    public void close() {
        client.close();
    }

    /**
     * Run a Hello CosmosDB console application.
     * <p>
     * This is a simple sample application intended to demonstrate Create, Read,
     * Update, Delete (CRUD) operations with Azure Cosmos DB Java SDK, as applied to
     * databases, containers and items. This sample will 1. Create synchronous
     * client, database and container instances 2. Create several items 3. Upsert
     * one of the items 4. Perform a query over the items 5. Delete an item 6.
     * Delete the Cosmos DB database and container resources and close the client. *
     */
    // <Main>
    public static void main(String[] args) {
        SamplePatchQuickstart p = new SamplePatchQuickstart();

        try {
            logger.info("Starting SYNC main");
            p.getStartedDemo();
            logger.info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            logger.error("Cosmos getStarted failed", e);
        } finally {
            logger.info("Closing the client");
            p.shutdown();
        }
    }

    // </Main>

    private void getStartedDemo() throws Exception {

        logger.info("Using Azure Cosmos DB endpoint {}", AccountSettings.HOST);
        // ArrayList<String> preferredRegions = new ArrayList<String>();
        // preferredRegions.add("West US");

        // Create sync client
        // <CreateSyncClient>
        client = new CosmosClientBuilder().endpoint(AccountSettings.HOST).key(AccountSettings.MASTER_KEY)
                // .preferredRegions(preferredRegions)
                .consistencyLevel(ConsistencyLevel.EVENTUAL).contentResponseOnWriteEnabled(true).buildClient();

        // </CreateSyncClient>

        createDatabaseIfNotExists();
        createContainerIfNotExists();

        // Setup family items to create
        ArrayList<Family> families = new ArrayList<>();
        families.add(Families.getAndersenFamilyItem());
        families.add(Families.getWakefieldFamilyItem());
        families.add(Families.getJohnsonFamilyItem());
        families.add(Families.getSmithFamilyItem());

        // Creates several items in the container
        // Also applies an upsert operation to one of the items (create if not present,
        // otherwise replace)
        createFamilies(families);

        patchAddSingle(families.get(0).getId(), families.get(0).getLastName());
        patchAddSingleJsonString(families.get(0).getId(), families.get(0).getLastName());
        patchAddMultiple(families.get(0).getId(), families.get(0).getLastName());
        patchAddArray(families.get(0).getId(), families.get(0).getLastName());
        patchSet(families.get(2).getId(), families.get(2).getLastName());
        patchReplace(families.get(0).getId(), families.get(0).getLastName());
        patchIncrement(families.get(0).getId(), families.get(0).getLastName());
        patchConditional(families.get(0).getId(), families.get(0).getLastName(), "from f where f.registered = false");
        patchTransactionalBatch();
        patchRemove(families.get(1).getId(), families.get(1).getLastName());
        patchBulk();
        patchTransactionalBatchAdvanced();
    }

    private void createDatabaseIfNotExists() throws Exception {
        logger.info("Create database {} if not exists.", databaseName);

        // Create database if not exists
        // <CreateDatabaseIfNotExists>
        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName);
        database = client.getDatabase(databaseResponse.getProperties().getId());

        // </CreateDatabaseIfNotExists>

        logger.info("Checking database {} completed!\n", database.getId());
    }

    private void createContainerIfNotExists() throws Exception {
        logger.info("Create container {} if not exists.", containerName);

        // Create container if not exists
        // <CreateContainerIfNotExists>
        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, "/lastName");

        // Create container with 400 RU/s
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);
        CosmosContainerResponse containerResponse = database.createContainerIfNotExists(containerProperties,
                throughputProperties);
        container = database.getContainer(containerResponse.getProperties().getId());
        // </CreateContainerIfNotExists>

        logger.info("Checking container {} completed!\n", container.getId());
    }

    private void createFamilies(List<Family> families) throws Exception {
        double totalRequestCharge = 0;
        for (Family family : families) {

            // <CreateItem>
            // Create item using container that we created using sync client

            // Use lastName as partitionKey for cosmos item
            // Using appropriate partition key improves the performance of database
            // operations
            CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions();
            CosmosItemResponse<Family> item = container.createItem(family, new PartitionKey(family.getLastName()),
                    cosmosItemRequestOptions);
            // </CreateItem>

            logger.info("Created item with request charge of {} within duration {}", item.getRequestCharge(),
                    item.getDuration());

            totalRequestCharge += item.getRequestCharge();
        }
        logger.info("Created {} items with total request charge of {}", families.size(), totalRequestCharge);

        Family family_to_upsert = families.get(0);
        logger.info("Upserting the item with id {} after modifying the isRegistered field...",
                family_to_upsert.getId());
        family_to_upsert.setRegistered(!family_to_upsert.isRegistered());

        CosmosItemResponse<Family> item = container.upsertItem(family_to_upsert);

        // Get upsert request charge and other properties like latency, and diagnostics
        // strings, etc.
        logger.info("Upserted item with request charge of {} within duration {}", item.getRequestCharge(),
                item.getDuration());
    }

    // demonstrates a single patch (add) operation
    private void patchAddSingle(String id, String partitionKey) {
        logger.info("Executing Patch with single 'add' operation");

        CosmosPatchOperations cosmosPatchOperations = CosmosPatchOperations.create();
        // attribute does not exist, will be added
        cosmosPatchOperations.add("/vaccinated", false);

        CosmosPatchItemRequestOptions options = new CosmosPatchItemRequestOptions();
        try {
            CosmosItemResponse<Family> response = this.container.patchItem(id, new PartitionKey(partitionKey),
                    cosmosPatchOperations, options, Family.class);
            logger.info("Item with ID {} has been patched", response.getItem().getId());
        } catch (Exception e) {
            logger.error("failed", e);
        }
    }

    // demonstrates a single patch (add) operation using json String
    private void patchAddSingleJsonString(String id, String partitionKey) {
        logger.info("Executing Patch with single 'add' operation to add json from String");
        String json = "[{\"givenName\": \"Goofy\"},{\"givenName\": \"Shadow\"}]";
        ObjectMapper objectMapper = new ObjectMapper();
        try {
        //Converting json String to JsonNode using Jackson
        JsonNode jsonNode = objectMapper.readTree(json);
        CosmosPatchOperations cosmosPatchOperations = CosmosPatchOperations.create();
        // attribute does not exist, will be added
        cosmosPatchOperations.add("/pets", jsonNode);

        CosmosPatchItemRequestOptions options = new CosmosPatchItemRequestOptions();

            CosmosItemResponse<Family> response = this.container.patchItem(id, new PartitionKey(partitionKey),
                    cosmosPatchOperations, options, Family.class);
            logger.info("Item with ID {} has been patched", response.getItem().getId());
        } catch (Exception e) {
            logger.error("failed", e);
        }
    }

    // demonstrates multiple patch (add) operations
    private void patchAddMultiple(String id, String partitionKey) {
        logger.info("Executing Patch with multiple 'add' operations");

        CosmosPatchOperations cosmosPatchOperations = CosmosPatchOperations.create();
        cosmosPatchOperations
                // attribute does not exist, will be added
                .add("/vaccinated", false)
                // attribute already exists, will be replaced
                .add("/district", "NY23");

        CosmosPatchItemRequestOptions options = new CosmosPatchItemRequestOptions();
        try {
            CosmosItemResponse<Family> response = this.container.patchItem(id, new PartitionKey(partitionKey),
                    cosmosPatchOperations, options, Family.class);
            logger.info("Item with ID {} has been patched", response.getItem().getId());
        } catch (Exception e) {
            logger.error("failed", e);
        }
    }

    // demonstrates multiple patch (add) operations for an array
    private void patchAddArray(String id, String partitionKey) {
        logger.info("Executing Patch with 'add' operation on array");

        CosmosPatchOperations cosmosPatchOperations = CosmosPatchOperations.create();

        Child c1 = new Child();
        c1.setFamilyName("Andersen");
        c1.setFirstName("Selena");
        c1.setGender("f");

        Child c2 = new Child();
        c2.setFamilyName("Andersen");
        c2.setFirstName("John");
        c2.setGender("m");

        // add to an empty array
        cosmosPatchOperations.add("/children", Arrays.asList(c1, c2));

        Child c3 = new Child();
        c3.setFamilyName("Andersen");
        c3.setFirstName("Shaun");
        c3.setGender("m");
        // add to an existing array to specific index. will add element at that index
        // and
        // shift others
        cosmosPatchOperations.add("/children/1", c3);

        Child c4 = new Child();
        c4.setFamilyName("Andersen");
        c4.setFirstName("Mariah");
        c4.setGender("f");

        // add to an existing array, with index equal to array length. element will be
        // added to end of array

        cosmosPatchOperations.add("/children/3", c4);

        Child c5 = new Child();
        c5.setFamilyName("Andersen");
        c5.setFirstName("Brian");
        c5.setGender("m");

        // to append to end of an array, you can use the convenience character "-". you
        // can skip index calculation
        cosmosPatchOperations.add("/children/-", c5);

        // un-commenting below will result in BadRequestException: array contains 4
        // elements now. an attempt to add element at an index that is greater than the
        // length

        // cosmosPatchOperations.add("/children/5", c4);

        CosmosPatchItemRequestOptions options = new CosmosPatchItemRequestOptions();
        try {
            CosmosItemResponse<Family> response = this.container.patchItem(id, new PartitionKey(partitionKey),
                    cosmosPatchOperations, options, Family.class);
            logger.info("Item with ID {} has been patched", response.getItem().getId());

        } catch (Exception e) {
            logger.error("failed", e);
        }
    }

    // demonstrates set operation. it is same as add except for array.
    private void patchSet(String id, String partitionKey) {
        logger.info("Executing Patch with 'set' operations");

        CosmosPatchOperations cosmosPatchOperations = CosmosPatchOperations.create();
        // does not exist, will be added (same behavior as add)
        cosmosPatchOperations.set("/vaccinated", false);
        // exists, will be replaced (same behavior as add)
        cosmosPatchOperations.set("/district", "WA5");

        Child c1 = new Child();
        c1.setFamilyName("Andersen");
        c1.setFirstName("Selena");
        c1.setGender("f");

        cosmosPatchOperations.set("/children", Arrays.asList(c1));

        Child c3 = new Child();
        c3.setFamilyName("Andersen");
        c3.setFirstName("Shaun");
        c3.setGender("m");

        // add to an existing array, with index. will substitute element at that index
        // (NOT the same behavior as add)
        cosmosPatchOperations.set("/children/0", c3);

        Child c4 = new Child();
        c4.setFamilyName("Andersen");
        c4.setFirstName("Mariah");
        c4.setGender("f");

        // add to an existing array, with index equal to array length. element will be
        // added to end of array
        cosmosPatchOperations.set("/children/1", c4);

        CosmosPatchItemRequestOptions options = new CosmosPatchItemRequestOptions();
        try {
            CosmosItemResponse<Family> response = this.container.patchItem(id, new PartitionKey(partitionKey),
                    cosmosPatchOperations, options, Family.class);
            logger.info("Item with ID {} has been patched", response.getItem().getId());
        } catch (Exception e) {
            logger.error("failed", e);
        }
    }

    // can be used to change value of an existing attribute. will fail if the
    // attribute does not exist
    private void patchReplace(String id, String partitionKey) {
        logger.info("Executing Patch with 'replace' operations");

        CosmosPatchOperations cosmosPatchOperations = CosmosPatchOperations.create();
        cosmosPatchOperations
                // the attribute exists, so replace will work
                .replace("/district", "new_replaced_value")
                // works for array, as expected
                .replace("/parents/0/familyName", "Andersen");

        // add to empty array. works as expected
        Child c1 = new Child();
        c1.setFamilyName("Andersen");
        c1.setFirstName("Selena");
        c1.setGender("f");

        cosmosPatchOperations.replace("/children", Arrays.asList(c1));

        // un-commenting below will result in BadRequestException: add to an existing
        // array, with index equal to (or more than) array length
        /*
         * Child c2 = new Child(); c2.setFamilyName("Andersen");
         * c2.setFirstName("John"); c2.setGender("f");
         * cosmosPatchOperations.replace("/children/1", c2);
         */

        // attribute does not exist. un-commenting below will cause exception.

        // cosmosPatchOperations.replace("/does_not_exist", "new value");

        CosmosPatchItemRequestOptions options = new CosmosPatchItemRequestOptions();
        try {
            CosmosItemResponse<Family> response = this.container.patchItem(id, new PartitionKey(partitionKey),
                    cosmosPatchOperations, options, Family.class);
            logger.info("Item with ID {} has been patched", response.getItem().getId());
        } catch (Exception e) {
            logger.error("failed", e);
        }
    }

    // demonstrates how to use a predicate for conditional update
    private void patchConditional(String id, String partitionKey, String predicate) {
        logger.info("Executing Conditional Patch with predicate");

        CosmosPatchOperations cosmosPatchOperations = CosmosPatchOperations.create();
        cosmosPatchOperations.add("/vaccinated", false).replace("/district", "new_replaced_value");

        CosmosPatchItemRequestOptions options = new CosmosPatchItemRequestOptions();

        logger.info("predicate: {}", predicate);
        options.setFilterPredicate(predicate);

        // predicate match failure will result in BadRequestException
        try {
            CosmosItemResponse<Family> response = this.container.patchItem(id, new PartitionKey(partitionKey),
                    cosmosPatchOperations, options, Family.class);
            logger.info("Item with ID {} has been patched", response.getItem().getId());
        } catch (Exception e) {
            logger.error("failed", e);
        }
    }

    private void patchIncrement(String id, String partitionKey) {
        logger.info("Executing Patch with 'increment' operations");

        CosmosPatchOperations cosmosPatchOperations = CosmosPatchOperations.create();
        cosmosPatchOperations
                // attribute does not exist, will be added
                .add("/int", 42)
                // attribute already exists, will be replaced
                .increment("/int", 1)
                // negative increment works is equivalent to decrement
                .increment("/int", -1)
                // floats work the same way
                .add("/float", 42).increment("/float", 4.2).increment("/float", -4.2);

        CosmosPatchItemRequestOptions options = new CosmosPatchItemRequestOptions();
        try {
            CosmosItemResponse<Family> response = this.container.patchItem(id, new PartitionKey(partitionKey),
                    cosmosPatchOperations, options, Family.class);

            logger.info("Item with ID {} has been patched", response.getItem().getId());
        } catch (Exception e) {
            logger.error("failed", e);
        }
    }

    // demonstrates how to remove an attribute, including array
    private void patchRemove(String id, String partitionKey) {
        logger.info("Executing Patch with 'remove' operations");

        CosmosPatchOperations cosmosPatchOperations = CosmosPatchOperations.create();

        cosmosPatchOperations
                // removes this attribute
                .remove("/registered")
                // removes the element from an array
                .remove("/children/0/pets/0");

        // un-commenting this will cause an exception since the attribute does not exist

        // cosmosPatchOperations.remove("/attribute_does_not_exist");

        CosmosPatchItemRequestOptions options = new CosmosPatchItemRequestOptions();
        try {
            CosmosItemResponse<Family> response = this.container.patchItem(id, new PartitionKey(partitionKey),
                    cosmosPatchOperations, options, Family.class);
            logger.info("Item with ID {} has been patched", response.getItem().getId());
        } catch (Exception e) {
            logger.error("failed", e);
        }
    }

    private void patchTransactionalBatch() {
        logger.info("Executing Patch operation as part of a Transactional Batch");

        Family family = Families.getAndersenFamilyItem();

        // TransactionalBatch batch = TransactionalBatch.createTransactionalBatch(new
        // PartitionKey(family.getLastName()));
        CosmosBatch batch = CosmosBatch.createCosmosBatch(new PartitionKey(family.getLastName()));

        batch.createItemOperation(family);

        CosmosPatchOperations cosmosPatchOperations = CosmosPatchOperations.create();
        cosmosPatchOperations.add("/vaccinated", false);

        batch.patchItemOperation(family.getId(), cosmosPatchOperations);

        try {
            CosmosBatchResponse response = container.executeCosmosBatch(batch);

            logger.info("Response code for transactional batch operation: ", response.getStatusCode());
            if (response.isSuccessStatusCode()) {
                logger.info("Transactional batch operation executed for ID {}",
                        response.getResults().get(0).getItem(Family.class).getId());
            }
        } catch (Exception e) {
            logger.error("failed", e);
        }
    }

    // demonstrates a variety of patch operation in the context of a transactional
    // batch - including a conditional predicate
    private void patchTransactionalBatchAdvanced() {
        logger.info("Executing Multiple Patch operations on multiple documents as part of a Transactional Batch");

        String lastName = "testLastName";

        // generating two random Family objects. these will be created and then patched
        // (within a transaction)
        Family family1 = Families.getWakefieldFamilyItem();
        Family family2 = Families.getJohnsonFamilyItem();

        // setting a fixed last name so that we can use it as a partition key for the
        // transactional batch
        family1.setLastName(lastName);
        family2.setLastName(lastName);

        // TransactionalBatch batch = TransactionalBatch.createTransactionalBatch(new
        // PartitionKey(lastName));
        CosmosBatch batch = CosmosBatch.createCosmosBatch(new PartitionKey(lastName));

        batch.createItemOperation(family1);
        batch.createItemOperation(family2);

        CosmosPatchOperations commonPatchOperation = CosmosPatchOperations.create();
        commonPatchOperation.replace("/district", "new_replaced_value");

        // replacing 'district' via patch
        batch.patchItemOperation(family1.getId(), commonPatchOperation);
        batch.patchItemOperation(family2.getId(), commonPatchOperation);

        // if registered is false (predicate/condition), vaccinated status has to be
        // false as well (defining this via conditional patch operation)
        CosmosPatchOperations addVaccinatedStatus = CosmosPatchOperations.create().add("/vaccinated", false);
        // TransactionalBatchPatchItemRequestOptions options = new
        // TransactionalBatchPatchItemRequestOptions();
        CosmosBatchPatchItemRequestOptions options = new CosmosBatchPatchItemRequestOptions();

        options.setFilterPredicate("from f where f.registered = false");

        batch.patchItemOperation(family2.getId(), addVaccinatedStatus, options);

        // the below patch operation will fail (error 412) since the
        // predicate/condition (registered = false) will not be satisfied. this in turn,
        // will result in the
        // transaction to fail as well (error 424)
        // if you want to see this, un-comment the line below

        // batch.patchItemOperation(family1.getId(), addVaccinatedStatus, options);

        try {
            CosmosBatchResponse response = container.executeCosmosBatch(batch);

            for (CosmosBatchOperationResult batchOpResult : response.getResults()) {

                if (response.isSuccessStatusCode()) {
                    logger.info("{} operation for ID {} was successful",
                            batchOpResult.getOperation().getOperationType().name(),
                            batchOpResult.getItem(Family.class).getId());
                } else {
                    logger.info("{} operation failed. Status code: {}",
                            batchOpResult.getOperation().getOperationType().name(), batchOpResult.getStatusCode());
                }
            }

        } catch (Exception e) {
            logger.error("failed", e);
        }
    }

    // this example shows heterogenous bulk operations (create, batch). we first
    // create an item and then patch it.
    private void patchBulk() {
        logger.info("Executing Patch operation in bulk");

        Family family = Families.getSmithFamilyItem();
        String partitionKey = family.getLastName();

        CosmosItemOperation createOperation = CosmosBulkOperations.getCreateItemOperation(family,
                new PartitionKey(partitionKey));

        CosmosItemOperation patchOperation = CosmosBulkOperations.getPatchItemOperation(family.getId(),
                new PartitionKey(partitionKey),
                CosmosPatchOperations.create().add("/vaccinated", false).replace("/district", "new_replaced_value"));

        try {
            Iterable<CosmosBulkOperationResponse<Family>> bulkOperationsResponses = container
                    .executeBulkOperations(Arrays.asList(createOperation, patchOperation));

            for (CosmosBulkOperationResponse<Family> response : bulkOperationsResponses) {
                String result = response.getResponse().isSuccessStatusCode() ? "was successful" : "failed";

                logger.info("{} operation for item with ID {} {}", response.getOperation().getOperationType(),
                        response.getResponse().getItem(Family.class).getId(), result);
            }
        } catch (Exception e) {
            logger.error("failed", e);
        }
    }

    private void shutdown() {
        try {
            // Clean shutdown
            logger.info("Deleting Cosmos DB resources");
            logger.info("-Deleting container...");
            if (container != null)
                 container.delete();
                logger.info("-Deleting database...");
            if (database != null)
                 database.delete();
                logger.info("-Closing the client...");
        } catch (Exception err) {
            logger.error(
                    "Deleting Cosmos DB resources failed, will still attempt to close the client. See stack trace below.",
                    err);

        }
        client.close();
        logger.info("Done.");
    }
}