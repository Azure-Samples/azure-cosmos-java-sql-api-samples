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
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedFlux;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalyticalContainerCRUDQuickstartAsync {

    private CosmosAsyncClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    protected static Logger logger = LoggerFactory.getLogger(AnalyticalContainerCRUDQuickstartAsync.class);

    public void close() {
        client.close();
    }

    /**
     * Sample to demonstrate the following ANALYTICAL STORE container CRUD
     * operations:
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
            logger.error("Cosmos getStarted failed", e);
        } finally {
            logger.info("Closing the client");
            p.shutdown();
        }
    }

    private void containerCRUDDemo() throws Exception {

        logger.info("Using Azure Cosmos DB endpoint: {}", AccountSettings.HOST);

        // Create async client
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .contentResponseOnWriteEnabled(true)
                .buildAsyncClient();

        createDatabaseIfNotExists();
        createContainerIfNotExists();
        updateContainerThroughput();

        readContainerById();
        readAllContainers();

        // deleteAContainer() is called at shutdown()

    }

    // Database Create
    private void createDatabaseIfNotExists() throws Exception {
        logger.info("Create database {} if not exists...", databaseName);

        // Create database if not exists - this is async but we block to make sure
        // database and containers are created before sample runs the CRUD operations on
        // them
        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName).block();
        database = client.getDatabase(databaseResponse.getProperties().getId());

        logger.info("Done.");
    }

    // Container create
    private void createContainerIfNotExists() throws Exception {
        logger.info("Create container " + containerName + " if not exists.");

        // Create container if not exists
        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, "/lastName");

        // Set analytical store properties
        containerProperties.setAnalyticalStoreTimeToLiveInSeconds(-1);

        // Create container - this is async but we block to make sure
        // database and containers are created before sample runs the CRUD operations on
        // them
        CosmosContainerResponse containerResponse = database.createContainerIfNotExists(containerProperties).block();
        CosmosAsyncContainer container = database.getContainer(containerResponse.getProperties().getId());

        logger.info("Done.");
    }

    // Update container throughput
    private void updateContainerThroughput() throws Exception {
        logger.info("Update throughput for container " + containerName + ".");

        // Specify new throughput value
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(800);
        container.replaceThroughput(throughputProperties).block();

        logger.info("Done.");
    }

    // Container read
    private void readContainerById() throws Exception {
        logger.info("Read container " + containerName + " by ID.");

        // Get container id
        container = database.getContainer(containerName);

        logger.info("container id: "+container.getId());
    }

    // Container read all
    private void readAllContainers() throws Exception {
        logger.info("Read all containers in database " + databaseName + ".");

        // Read all containers in the account
        CosmosPagedFlux<CosmosContainerProperties> containers = database.readAllContainers();

        // Print
        String msg = "Listing containers in database:\n";
        containers.byPage(100).flatMap(readAllContainersResponse -> {
            logger.info("read " +
                    readAllContainersResponse.getResults().size() + " containers(s)"
                    + " with request charge of " + readAllContainersResponse.getRequestCharge());

            for (CosmosContainerProperties response : readAllContainersResponse.getResults()) {
                logger.info("container id: " + response.getId());
                // Got a page of query result with
            }
            return Flux.empty();
        }).blockLast();
        // for(CosmosContainerProperties containerProps : containers) {
        // msg += String.format("-Container ID: %s\n",containerProps.getId());
        // }
        logger.info(msg + "\n");

        logger.info("Done.");
    }

    // Container delete
    private void deleteAContainer() throws Exception {
        logger.info("Delete container " + containerName + " by ID.");

        // Delete container
        CosmosContainerResponse containerResp = database.getContainer(containerName)
                .delete().doOnError(throwable -> {
                    logger.warn("Delete container {} failed ", containerName, throwable);                    
                }).doOnSuccess(response -> {
                    logger.info("Delete container {} succeeded: {}", containerName);
                }).block();
        logger.info("Status code for container delete: {}", containerResp.getStatusCode());

        logger.info("Done.");
    }

    // Database delete
    private void deleteADatabase() throws Exception {
        logger.info("Last step: delete database " + databaseName + " by ID.");

        // Delete database
        Mono<CosmosDatabaseResponse> dbResp = client.getDatabase(databaseName)
                .delete(new CosmosDatabaseRequestOptions());
        logger.info("Status code for database delete: {}", dbResp.block().getStatusCode());

        logger.info("Done.");
    }

    // Cleanup before close
    private void shutdown() {
        try {
            // Clean shutdown
            deleteAContainer();
            deleteADatabase();
        } catch (Exception err) {
            logger.error(
                    "Deleting Cosmos DB resources failed, will still attempt to close the client. See stack trace below.");
            err.printStackTrace();
        }
        client.close();
        logger.info("Done with sample.");
    }

}
