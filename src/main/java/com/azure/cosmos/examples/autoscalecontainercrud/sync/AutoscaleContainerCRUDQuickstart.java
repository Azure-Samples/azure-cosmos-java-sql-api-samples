// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.autoscalecontainercrud.sync;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerRequestOptions;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseRequestOptions;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoscaleContainerCRUDQuickstart {

    private CosmosClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";

    private CosmosDatabase database;
    private CosmosContainer container;

    protected static Logger logger = LoggerFactory.getLogger(AutoscaleContainerCRUDQuickstart.class);

    public void close() {
        client.close();
    }

    /**
     * Sample to demonstrate the following AUTOSCALE container CRUD operations:
     * -Create
     * -Update throughput
     * -Read by ID
     * -Read all
     * -Delete
     */
    public static void main(String[] args) {
        AutoscaleContainerCRUDQuickstart p = new AutoscaleContainerCRUDQuickstart();

        try {
            logger.info("Starting SYNC main");
            p.autoscaleContainerCRUDDemo();
            logger.info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(String.format("Cosmos getStarted failed with %s", e));
        } finally {
            logger.info("Closing the client");
            p.shutdown();
        }
    }

    private void autoscaleContainerCRUDDemo() throws Exception {

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
        logger.info("Create autoscale container " + containerName + " if not exists.");

        // Container and autoscale throughput settings
        CosmosContainerProperties autoscaleContainerProperties = new CosmosContainerProperties(containerName, "/lastName");
        ThroughputProperties autoscaleThroughputProperties = ThroughputProperties.createAutoscaledThroughput(4000); //Set autoscale max RU/s

        // Create the container with autoscale enabled
        CosmosContainerResponse databaseResponse = database.createContainer(autoscaleContainerProperties, autoscaleThroughputProperties,
                new CosmosContainerRequestOptions());
        container = database.getContainer(databaseResponse.getProperties().getId());

        logger.info("Done.");
    }

    // Update container throughput
    private void updateContainerThroughput() throws Exception {
        logger.info("Update autoscale max throughput for container " + containerName + ".");

        // Change the autoscale max throughput (RU/s)
        container.replaceThroughput(ThroughputProperties.createAutoscaledThroughput(8000));

        logger.info("Done.");
    }

    private void readContainerThroughput() throws Exception {
        // Read the throughput on a resource
        ThroughputProperties autoscaleContainerThroughput = container.readThroughput().getProperties();

        // The autoscale max throughput (RU/s) of the resource
        int autoscaleMaxThroughput = autoscaleContainerThroughput.getAutoscaleMaxThroughput();

        // The throughput (RU/s) the resource is currently scaled to
        //int currentThroughput = autoscaleContainerThroughput.Throughput;

        //logger.info("Autoscale max throughput: {} current throughput: {}",autoscaleMaxThroughput,currentThroughput);
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
