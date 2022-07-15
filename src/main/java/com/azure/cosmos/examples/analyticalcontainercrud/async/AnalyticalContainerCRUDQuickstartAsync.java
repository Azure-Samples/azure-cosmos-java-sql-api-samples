// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.analyticalcontainercrud.async;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerRequestOptions;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseRequestOptions;
import com.azure.cosmos.models.CosmosDatabaseResponse;

import reactor.core.publisher.Mono;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalyticalContainerCRUDQuickstartAsync {

    private CosmosAsyncClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";

    private CosmosAsyncDatabase database;

    protected static Logger logger = LoggerFactory.getLogger(AnalyticalContainerCRUDQuickstartAsync.class);

    public void close() {
        client.close();
    }

    /**
     * Sample to demonstrate the following ANALYTICAL STORE container CRUD operations:
     * -Create
     * -Update throughput
     * -Read by ID
     * -Read all
     * -Delete
     */
    public static void main(String[] args) {
        AnalyticalContainerCRUDQuickstartAsync p = new AnalyticalContainerCRUDQuickstartAsync();

        try {
            logger.info("Starting ASYNC main");
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
                .buildAsyncClient();


        createDatabaseIfNotExists();
        createContainerIfNotExists();

        // deleteAContainer() is called at shutdown()

    }

    // Database Create
    private void createDatabaseIfNotExists() throws Exception {
        logger.info("Create database " + databaseName + " if not exists...");

        //  Create database if not exists
        Mono<CosmosDatabaseResponse> databaseResponse = client.createDatabaseIfNotExists(databaseName);
        database = client.getDatabase(databaseResponse.block().getProperties().getId());

        logger.info("Done.");
    }

    // Container create
    private void createContainerIfNotExists() throws Exception {
        logger.info("Create container " + containerName + " if not exists.");

        //  Create container if not exists
        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(containerName, "/lastName");

        // Set analytical store properties
        containerProperties.setAnalyticalStoreTimeToLiveInSeconds(-1);

        //  Create container
        Mono<CosmosContainerResponse> databaseResponse = database.createContainerIfNotExists(containerProperties);
        CosmosAsyncContainer container = database.getContainer(databaseResponse.block().getProperties().getId());

        logger.info("Done.");
    }

    // Container delete
    private void deleteAContainer() throws Exception {
        logger.info("Delete container " + containerName + " by ID.");

        // Delete container
        Mono<CosmosContainerResponse> containerResp = database.getContainer(containerName).delete(new CosmosContainerRequestOptions());
        logger.info("Status code for container delete: {}",containerResp.block().getStatusCode());

        logger.info("Done.");
    }

    // Database delete
    private void deleteADatabase() throws Exception {
        logger.info("Last step: delete database " + databaseName + " by ID.");

        // Delete database
        Mono<CosmosDatabaseResponse> dbResp = client.getDatabase(databaseName).delete(new CosmosDatabaseRequestOptions());
        logger.info("Status code for database delete: {}",dbResp.block().getStatusCode());

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
