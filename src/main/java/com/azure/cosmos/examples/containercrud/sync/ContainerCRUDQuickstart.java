// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.containercrud.sync;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.CosmosPagedIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerCRUDQuickstart {

    private CosmosClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";

    private CosmosDatabase database;
    private CosmosContainer container;

    protected static Logger logger = LoggerFactory.getLogger(ContainerCRUDQuickstart.class);

    public void close() {
        client.close();
    }

    /**
     * Sample to demonstrate the following container CRUD operations:
     * -Create
     * -Update throughput
     * -Read by ID
     * -Read all
     * -Delete
     */
    public static void main(String[] args) {
        ContainerCRUDQuickstart p = new ContainerCRUDQuickstart();

        try {
            logger.info("Starting SYNC main");
            p.containerCRUDDemo();
            logger.info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(String.format("Cosmos getStarted failed with %s", e));
        } finally {
            logger.info("Closing the client");
            p.shutdown();
        }
    }

    private void containerCRUDDemo() throws Exception {

        logger.info("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        //  Create sync client
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .contentResponseOnWriteEnabled(true)
                .buildClient();


        createDatabaseIfNotExists();
        createContainerIfNotExists();
        updateContainerThroughput();

        readContainerById();
        readAllContainers();
        // deleteAContainer() is called at shutdown()

    }

    // Database Create
    private void createDatabaseIfNotExists() throws Exception {
        logger.info("Create database " + databaseName + " if not exists...");

        //  Create database if not exists
        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName);
        database = client.getDatabase(databaseResponse.getProperties().getId());

        logger.info("Done.");
    }

    // Container create
    private void createContainerIfNotExists() throws Exception {
        logger.info("Create container " + containerName + " if not exists.");

        //  Create container if not exists
        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(containerName, "/lastName");

        // Provision throughput
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);

        //  Create container with 400 RU/s
        CosmosContainerResponse databaseResponse = database.createContainerIfNotExists(containerProperties, throughputProperties);
        container = database.getContainer(databaseResponse.getProperties().getId());

        logger.info("Done.");
    }

    // Update container throughput
    private void updateContainerThroughput() throws Exception {
        logger.info("Update throughput for container " + containerName + ".");

        // Specify new throughput value
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(800);
        container.replaceThroughput(throughputProperties);

        logger.info("Done.");
    }

    // Container read
    private void readContainerById() throws Exception {
        logger.info("Read container " + containerName + " by ID.");

        //  Read container by ID
        container = database.getContainer(containerName);

        logger.info("Done.");
    }

    // Container read all
    private void readAllContainers() throws Exception {
        logger.info("Read all containers in database " + databaseName + ".");

        //  Read all containers in the account
        CosmosPagedIterable<CosmosContainerProperties> containers = database.readAllContainers();

        // Print
        String msg="Listing containers in database:\n";
        for(CosmosContainerProperties containerProps : containers) {
            msg += String.format("-Container ID: %s\n",containerProps.getId());
        }
        logger.info(msg + "\n");

        logger.info("Done.");
    }

    // Container delete
    private void deleteAContainer() throws Exception {
        logger.info("Delete container " + containerName + " by ID.");

        // Delete container
        CosmosContainerResponse containerResp = database.getContainer(containerName).delete(new CosmosContainerRequestOptions());
        logger.info("Status code for container delete: {}",containerResp.getStatusCode());

        logger.info("Done.");
    }

    // Database delete
    private void deleteADatabase() throws Exception {
        logger.info("Last step: delete database " + databaseName + " by ID.");

        // Delete database
        CosmosDatabaseResponse dbResp = client.getDatabase(databaseName).delete(new CosmosDatabaseRequestOptions());
        logger.info("Status code for database delete: {}",dbResp.getStatusCode());

        logger.info("Done.");
    }

    // Multi-master only: Container create: last-writer-wins conflict resolution
    // Favor the newest write based on default (internal) timestamp
    private void createContainerIfNotExistsLWWDefaultTimestamp() throws Exception {
        logger.info("Create container " + containerName + " if not exists.");

        //  Create container properties structure
        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(containerName, "/lastName");

        // Define LWW policy
        ConflictResolutionPolicy policy = ConflictResolutionPolicy.createLastWriterWinsPolicy();
        containerProperties.setConflictResolutionPolicy(policy);

        // Provision throughput
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);

        //  Create container with 400 RU/s
        CosmosContainerResponse databaseResponse = database.createContainerIfNotExists(containerProperties, throughputProperties);
        container = database.getContainer(databaseResponse.getProperties().getId());

        logger.info("Done.");
    }

    // Multi-master only: Container create: last-writer-wins conflict resolution
    // Favor the newest write, using one of the document fields as a custom timestamp
    private void createContainerIfNotExistsLWWCustomTimestamp() throws Exception {
        logger.info("Create container " + containerName + " if not exists.");

        //  Create container properties structure
        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(containerName, "/lastName");

        // Define LWW policy
        ConflictResolutionPolicy policy = ConflictResolutionPolicy.createLastWriterWinsPolicy("/customTimestamp");
        containerProperties.setConflictResolutionPolicy(policy);

        // Provision throughput
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);

        //  Create container with 400 RU/s
        CosmosContainerResponse databaseResponse = database.createContainerIfNotExists(containerProperties, throughputProperties);
        container = database.getContainer(databaseResponse.getProperties().getId());

        logger.info("Done.");
    }

    // Multi-master only: Container create: fully custom conflict resolution policy using stored procedure
    // Implement a custom conflict resolution policy, using a stored procedure to merge conflicts
    // A sample resolver.js stored procedure is adjacent in the sample directory.
    // You will need to upload this stored procedure to the back-end using either
    // the stored procedure sample in this repo, or the Azure portal.
    private void createContainerIfNotExistsCustomSproc() throws Exception {
        logger.info("Create container " + containerName + " if not exists.");

        //  Create container properties structure
        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(containerName, "/lastName");

        // Define Custom conflict resolution stored procedure (sproc)
        ConflictResolutionPolicy policy = ConflictResolutionPolicy.createCustomPolicy("resolver");
        containerProperties.setConflictResolutionPolicy(policy);

        // Provision throughput
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);

        //  Create container with 400 RU/s
        CosmosContainerResponse databaseResponse = database.createContainerIfNotExists(containerProperties, throughputProperties);
        container = database.getContainer(databaseResponse.getProperties().getId());

        logger.info("Done.");
    }

    // Multi-master only: Container create: fully custom conflict resolution policy using conflict feed
    // Implement a custom conflict resolution policy; client pulls conflict from the conflict
    // feed and handles conflict resolution.
    // This method will register the container as using conflict feed;
    // a separate method is needed to actually monitor the conflict feed.
    private void createContainerIfNotExistsCustomConflictFeed() throws Exception {
        logger.info("Create container " + containerName + " if not exists.");

        //  Create container properties structure
        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(containerName, "/lastName");

        // Define policy indicating conflict-feed-based resolution
        ConflictResolutionPolicy policy = ConflictResolutionPolicy.createCustomPolicy();
        containerProperties.setConflictResolutionPolicy(policy);

        // Provision throughput
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);

        //  Create container with 400 RU/s
        CosmosContainerResponse databaseResponse = database.createContainerIfNotExists(containerProperties, throughputProperties);
        container = database.getContainer(databaseResponse.getProperties().getId());

        logger.info("Done.");
    }

    // Cleanup before close
    private void shutdown() {
        try {
            //Clean shutdown
            deleteAContainer();
            deleteADatabase();
        } catch (Exception err) {
            logger.error("Deleting Cosmos DB resources failed, will still attempt to close the client. See stack trace below.");
            err.printStackTrace();
        }
        client.close();
        logger.info("Done with sample.");
    }

}
