// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.autoscalecontainercrud.async;

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

public class AutoscaleContainerCRUDQuickstartAsync {

    private CosmosAsyncClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    private static final Logger logger = LoggerFactory.getLogger(AutoscaleContainerCRUDQuickstartAsync.class);

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
        AutoscaleContainerCRUDQuickstartAsync p = new AutoscaleContainerCRUDQuickstartAsync();

        try {
            logger.info("Starting ASYNC main");
            p.autoscaleContainerCRUDDemo();
            logger.info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Cosmos getStarted failed with {}", e);
        } finally {
            logger.info("Closing the client");
            p.shutdown();
        }
    }

    private void autoscaleContainerCRUDDemo() throws Exception {

        logger.info("Using Azure Cosmos DB endpoint: {}", AccountSettings.HOST);

        //  Create async client
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

        //  Create database if not exists
        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName).block();
        database = client.getDatabase(databaseResponse.getProperties().getId());

        logger.info("Done.");
    }

    // Container create
    private void createContainerIfNotExists() throws Exception {
        logger.info("Create autoscale container {} if not exists.", containerName);

        // Container and autoscale throughput settings
        CosmosContainerProperties autoscaleContainerProperties = new CosmosContainerProperties(containerName, "/lastName");
        ThroughputProperties autoscaleThroughputProperties = ThroughputProperties.createAutoscaledThroughput(4000); //Set autoscale max RU/s

        // Create the container with autoscale enabled
        CosmosContainerResponse databaseResponse = database.createContainer(autoscaleContainerProperties, autoscaleThroughputProperties,
                new CosmosContainerRequestOptions()).block();
        container = database.getContainer(databaseResponse.getProperties().getId());

        logger.info("Done.");
    }

    // Update container throughput
    private void updateContainerThroughput() throws Exception {
        logger.info("Update autoscale max throughput for container {}.", containerName);

        // Change the autoscale max throughput (RU/s)
        container.replaceThroughput(ThroughputProperties.createAutoscaledThroughput(8000)).block();

        logger.info("Done.");
    }

    private void readContainerThroughput() throws Exception {
        // Read the throughput on a resource
        ThroughputProperties autoscaleContainerThroughput = container.readThroughput().block().getProperties();

        // The autoscale max throughput (RU/s) of the resource
        int autoscaleMaxThroughput = autoscaleContainerThroughput.getAutoscaleMaxThroughput();

        // The throughput (RU/s) the resource is currently scaled to
        //int currentThroughput = autoscaleContainerThroughput.Throughput;

        //logger.info("Autoscale max throughput: {} current throughput: {}",autoscaleMaxThroughput,currentThroughput);
    }

    // Container read
    private void readContainerById() throws Exception {
        logger.info("Read container {} by ID.", containerName);

        //  Read container by ID
        container = database.getContainer(containerName);

        logger.info("Done.");
    }

    // Container read all
    private void readAllContainers() throws Exception {
        logger.info("Read all containers in database {}.", databaseName);

        //  Read all containers in the account
        CosmosPagedFlux<CosmosContainerProperties> containers = database.readAllContainers();

        // Print
        String msg="Listing containers in database:\n";
        containers.byPage(100).flatMap(readAllContainersResponse -> {
            logger.info("read {} containers(s) with request charge of {}", readAllContainersResponse.getResults().size(),readAllContainersResponse.getRequestCharge());

            for (CosmosContainerProperties response : readAllContainersResponse.getResults()) {
                logger.info("container id: {}",response.getId());
                //Got a page of query result with
            }
            return Flux.empty();
        }).blockLast(); 
        logger.info(msg + "\n");

        logger.info("Done.");
    }

    // Container delete
    private void deleteAContainer() throws Exception {
        logger.info("Delete container {} by ID.", containerName);

        // Delete container
        CosmosContainerResponse containerResp = database.getContainer(containerName).delete(new CosmosContainerRequestOptions()).block();
        logger.info("Status code for container delete: {}",containerResp.getStatusCode());

        logger.info("Done.");
    }

    // Database delete
    private void deleteADatabase() throws Exception {
        logger.info("Last step: delete database {} by ID.", databaseName);

        // Delete database
        CosmosDatabaseResponse dbResp = client.getDatabase(databaseName).delete(new CosmosDatabaseRequestOptions()).block();
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
        finally{
            client.close();
            logger.info("Done with sample.");
        }
    }

}
