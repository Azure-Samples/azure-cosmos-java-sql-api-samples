// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.patch.sync;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.CosmosPatchOperations;
import com.azure.cosmos.TransactionalBatch;
import com.azure.cosmos.TransactionalBatchResponse;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Families;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.examples.common.Child;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosPatchItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedIterable;
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

    protected static Logger logger = LoggerFactory.getLogger(SamplePatchQuickstart.class);

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
            e.printStackTrace();
            logger.error(String.format("Cosmos getStarted failed with %s", e));
        } finally {
            logger.info("Closing the client");
            p.shutdown();
        }
    }

    // </Main>

    private void getStartedDemo() throws Exception {

        logger.info("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        ArrayList<String> preferredRegions = new ArrayList<String>();
        preferredRegions.add("West US");

        // Create sync client
        // <CreateSyncClient>
        client = new CosmosClientBuilder().endpoint(AccountSettings.HOST).key(AccountSettings.MASTER_KEY)
                .preferredRegions(preferredRegions).consistencyLevel(ConsistencyLevel.EVENTUAL)
                .contentResponseOnWriteEnabled(true).buildClient();

        // </CreateSyncClient>

        createDatabaseIfNotExists();
        createContainerIfNotExists();

        // Setup family items to create
        ArrayList<Family> familiesToCreate = new ArrayList<>();
        familiesToCreate.add(Families.getAndersenFamilyItem());
        familiesToCreate.add(Families.getWakefieldFamilyItem());
        familiesToCreate.add(Families.getJohnsonFamilyItem());
        familiesToCreate.add(Families.getSmithFamilyItem());

        // Creates several items in the container
        // Also applies an upsert operation to one of the items (create if not present,
        // otherwise replace)
        createFamilies(familiesToCreate);

        patchAddSingle(familiesToCreate.get(0).getId(), familiesToCreate.get(0).getLastName());
        patchAddMultiple(familiesToCreate.get(0).getId(), familiesToCreate.get(0).getLastName());
        patchAddArray(familiesToCreate.get(0).getId(), familiesToCreate.get(0).getLastName());
        patchSet(familiesToCreate.get(2).getId(), familiesToCreate.get(2).getLastName());
        patchReplace(familiesToCreate.get(0).getId(), familiesToCreate.get(0).getLastName());
        patchIncrement(familiesToCreate.get(0).getId(), familiesToCreate.get(0).getLastName());
        patchConditional(familiesToCreate.get(0).getId(), familiesToCreate.get(0).getLastName(),
                "from f where f.registered = false");
        patchTransactionalBatch();

        patchRemove(familiesToCreate.get(1).getId(), familiesToCreate.get(1).getLastName());
    }

    private void createDatabaseIfNotExists() throws Exception {
        logger.info("Create database " + databaseName + " if not exists.");

        // Create database if not exists
        // <CreateDatabaseIfNotExists>
        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName);
        database = client.getDatabase(databaseResponse.getProperties().getId());

        // </CreateDatabaseIfNotExists>

        logger.info("Checking database " + database.getId() + " completed!\n");
    }

    private void createContainerIfNotExists() throws Exception {
        logger.info("Create container " + containerName + " if not exists.");

        // Create container if not exists
        // <CreateContainerIfNotExists>
        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, "/lastName");

        // Create container with 400 RU/s
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);
        CosmosContainerResponse containerResponse = database.createContainerIfNotExists(containerProperties,
                throughputProperties);
        container = database.getContainer(containerResponse.getProperties().getId());
        // </CreateContainerIfNotExists>

        logger.info("Checking container " + container.getId() + " completed!\n");
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

            // Get request charge and other properties like latency, and diagnostics
            // strings, etc.
            logger.info(String.format("Created item with request charge of %.2f within duration %s",
                    item.getRequestCharge(), item.getDuration()));

            totalRequestCharge += item.getRequestCharge();
        }
        logger.info(String.format("Created %d items with total request charge of %.2f", families.size(),
                totalRequestCharge));

        Family family_to_upsert = families.get(0);
        logger.info(String.format("Upserting the item with id %s after modifying the isRegistered field...",
                family_to_upsert.getId()));
        family_to_upsert.setRegistered(!family_to_upsert.isRegistered());

        CosmosItemResponse<Family> item = container.upsertItem(family_to_upsert);

        // Get upsert request charge and other properties like latency, and diagnostics
        // strings, etc.
        logger.info(String.format("Upserted item with request charge of %.2f within duration %s",
                item.getRequestCharge(), item.getDuration()));
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
            logger.info("Item with ID " + response.getItem().getId() + " has been patched");
        } catch (Exception e) {
            e.printStackTrace();
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
            logger.info("Item with ID " + response.getItem().getId() + " has been patched");
        } catch (Exception e) {
            e.printStackTrace();
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

        // un-commenting below will result in BadRequestException: array contains 4
        // elements now. an attempt to add element at an index that is greater than the
        // length

        // cosmosPatchOperations.add("/children/5", c4);

        CosmosPatchItemRequestOptions options = new CosmosPatchItemRequestOptions();
        try {
            CosmosItemResponse<Family> response = this.container.patchItem(id, new PartitionKey(partitionKey),
                    cosmosPatchOperations, options, Family.class);
            logger.info("Item with ID " + response.getItem().getId() + " has been patched");
        } catch (Exception e) {
            e.printStackTrace();
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
            logger.info("Item with ID " + response.getItem().getId() + " has been patched");
        } catch (Exception e) {
            e.printStackTrace();
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
            logger.info("Item with ID " + response.getItem().getId() + " has been patched");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // demonstrates how to use a predicate for conditional update
    private void patchConditional(String id, String partitionKey, String predicate) {
        logger.info("Executing Conditional Patch with predicate");

        CosmosPatchOperations cosmosPatchOperations = CosmosPatchOperations.create();
        cosmosPatchOperations.add("/vaccinated", false).replace("/district", "new_replaced_value");

        CosmosPatchItemRequestOptions options = new CosmosPatchItemRequestOptions();

        logger.info("predicate " + predicate);
        options.setFilterPredicate(predicate);

        // predicate match failure will result in BadRequestException
        try {
            CosmosItemResponse<Family> response = this.container.patchItem(id, new PartitionKey(partitionKey),
                    cosmosPatchOperations, options, Family.class);
            logger.info("Item with ID " + response.getItem().getId() + " has been patched");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void patchIncrement(String id, String partitionKey) {
        logger.info("Executing Patch with 'increment' operations");

        CosmosPatchOperations cosmosPatchOperations = CosmosPatchOperations.create();
        cosmosPatchOperations
                // attribute does not exist, will be added
                .add("/num", 42)
                // attribute already exists, will be replaced
                .increment("/num", 1)
                // negative increment works is equivalent to decrement
                .increment("/num", -1);

        CosmosPatchItemRequestOptions options = new CosmosPatchItemRequestOptions();
        try {
            CosmosItemResponse<Family> response = this.container.patchItem(id, new PartitionKey(partitionKey),
                    cosmosPatchOperations, options, Family.class);
            logger.info("Item with ID " + response.getItem().getId() + " has been patched");
        } catch (Exception e) {
            e.printStackTrace();
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
            logger.info("Item with ID " + response.getItem().getId() + " has been patched");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void patchTransactionalBatch() {
        logger.info("Executing Patch operation as part of a Transactional Batch");

        Family family = Families.getAndersenFamilyItem();

        TransactionalBatch batch = TransactionalBatch.createTransactionalBatch(new PartitionKey(family.getLastName()));

        batch.createItemOperation(family);

        CosmosPatchOperations cosmosPatchOperations = CosmosPatchOperations.create();
        cosmosPatchOperations.add("/vaccinated", false);

        batch.patchItemOperation(family.getId(), cosmosPatchOperations);

        try {
            TransactionalBatchResponse response = container.executeTransactionalBatch(batch);

            logger.info("Response code for transactional batch operation: " + response.getStatusCode());
            if (response.isSuccessStatusCode()) {
                logger.info("Transactional batch operation executed for ID "
                        + response.getResults().get(0).getItem(Family.class).getId());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void shutdown() {
        try {
            // Clean shutdown
            logger.info("Deleting Cosmos DB resources");
            logger.info("-Deleting container...");
            if (container != null)
                container.delete(
                logger.info("-Deleting database...");
            if (database != null)
                database.delete(
                logger.info("-Closing the client...");
        } catch (Exception err) {
            logger.error(
                    "Deleting Cosmos DB resources failed, will still attempt to close the client. See stack trace below.");
            err.printStackTrace();
        }
        client.close();
        logger.info("Done.");
    }
}