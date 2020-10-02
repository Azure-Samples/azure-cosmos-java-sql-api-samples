// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.diagnostics.async;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDiagnostics;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseRequestOptions;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedFlux;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

public class CosmosDiagnosticsQuickStartAsync {

    private CosmosAsyncClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";
    private final String documentId = UUID.randomUUID().toString();
    private final String documentLastName = "Witherspoon";

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    private final static Logger logger = LoggerFactory.getLogger(CosmosDiagnosticsQuickStartAsync.class);

    public void close() {
        client.close();
    }

    public static void main(String[] args) {
        CosmosDiagnosticsQuickStartAsync quickStart = new CosmosDiagnosticsQuickStartAsync();

        try {
            logger.info("Starting ASYNC main");
            quickStart.diagnosticsDemo();
            logger.info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            logger.error("Cosmos getStarted failed with", e);
        } finally {
            logger.info("Shutting down");
            quickStart.shutdown();
        }
    }

    private void diagnosticsDemo() throws Exception {

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

        createDocument();
        readDocumentById();
        queryDocuments();
        replaceDocument();
        upsertDocument();
    }

    // Database Diagnostics
    private void createDatabaseIfNotExists() throws Exception {
        logger.info("Creating database " + databaseName + " if not exists...");

        //  Create database if not exists
        Mono<CosmosDatabaseResponse> databaseResponseMono = client.createDatabaseIfNotExists(databaseName);
        CosmosDatabaseResponse cosmosDatabaseResponse = databaseResponseMono.flatMap(databaseResponse -> {
            CosmosDiagnostics diagnostics = databaseResponse.getDiagnostics();
            logger.info("Create database diagnostics : {}", diagnostics);
            return Mono.just(databaseResponse);
        }).block();

        database = client.getDatabase(cosmosDatabaseResponse.getProperties().getId());

        logger.info("Done.");
    }

    // Container create
    private void createContainerIfNotExists() throws Exception {
        logger.info("Creating container " + containerName + " if not exists.");

        //  Create container if not exists
        CosmosContainerProperties containerProperties =
            new CosmosContainerProperties(containerName, "/lastName");

        // Provision throughput
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);

        //  Create container with 200 RU/s
        Mono<CosmosContainerResponse> containerResponseMono = database.createContainerIfNotExists(containerProperties,
            throughputProperties);
        CosmosContainerResponse cosmosContainerResponse = containerResponseMono.flatMap(containerResponse -> {
            CosmosDiagnostics diagnostics = containerResponse.getDiagnostics();
            logger.info("Create container diagnostics : {}", diagnostics);
            return Mono.just(containerResponse);
        }).block();
        container = database.getContainer(cosmosContainerResponse.getProperties().getId());

        logger.info("Done.");
    }

    private void createDocument() throws Exception {
        logger.info("Create document " + documentId);

        // Define a document as a POJO (internally this
        // is converted to JSON via custom serialization)
        Family family = new Family();
        family.setLastName(documentLastName);
        family.setId(documentId);

        // Insert this item as a document
        // Explicitly specifying the /pk value improves performance.
        Mono<CosmosItemResponse<Family>> itemResponseMono = container.createItem(family,
            new PartitionKey(family.getLastName()),
            new CosmosItemRequestOptions());

        itemResponseMono.flatMap(itemResponse -> {
            CosmosDiagnostics diagnostics = itemResponse.getDiagnostics();
            logger.info("Create item diagnostics : {}", diagnostics);
            return Mono.just(itemResponse);
        }).block();

        logger.info("Done.");
    }

    // Document read
    private void readDocumentById() throws Exception {
        logger.info("Read document " + documentId + " by ID.");

        //  Read document by ID
        Mono<CosmosItemResponse<Family>> itemResponseMono = container.readItem(documentId,
            new PartitionKey(documentLastName), Family.class);

        CosmosItemResponse<Family> familyCosmosItemResponse = itemResponseMono.flatMap(itemResponse -> {
            CosmosDiagnostics diagnostics = itemResponse.getDiagnostics();
            logger.info("Read item diagnostics : {}", diagnostics);
            return Mono.just(itemResponse);
        }).block();

        Family family = familyCosmosItemResponse.getItem();

        // Check result
        logger.info("Finished reading family " + family.getId() + " with partition key " + family.getLastName());

        logger.info("Done.");
    }

    private void queryDocuments() throws Exception {
        logger.info("Query documents in the container " + containerName + ".");

        String sql = "SELECT * FROM c WHERE c.lastName = 'Witherspoon'";

        CosmosPagedFlux<Family> filteredFamilies = container.queryItems(sql, new CosmosQueryRequestOptions(),
            Family.class);

        //  Add handler to capture diagnostics
        filteredFamilies = filteredFamilies.handle(familyFeedResponse -> {
            logger.info("Query Item diagnostics through handler : {}", familyFeedResponse.getCosmosDiagnostics());
        });

        //  Or capture diagnostics through byPage() APIs.
        filteredFamilies.byPage().flatMap(familyFeedResponse -> {
            logger.info("Query item diagnostics through iterableByPage : {}",
                familyFeedResponse.getCosmosDiagnostics());
            return Flux.just(familyFeedResponse);
        }).blockLast();

        logger.info("Done.");
    }

    private void replaceDocument() throws Exception {
        logger.info("Replace document " + documentId);

        // Replace existing document with new modified document
        Family family = new Family();
        family.setLastName(documentLastName);
        family.setId(documentId);
        family.setDistrict("Columbia"); // Document modification

        Mono<CosmosItemResponse<Family>> itemResponseMono =
            container.replaceItem(family, family.getId(), new PartitionKey(family.getLastName()),
                new CosmosItemRequestOptions());

        itemResponseMono.flatMap(itemResponse -> {
            CosmosDiagnostics diagnostics = itemResponse.getDiagnostics();
            logger.info("Replace item diagnostics : {}", diagnostics);
            logger.info("Request charge of replace operation: {} RU", itemResponse.getRequestCharge());
            return Mono.just(itemResponse);
        }).block();

        logger.info("Done.");
    }

    private void upsertDocument() throws Exception {
        logger.info("Replace document " + documentId);

        // Replace existing document with new modified document (contingent on modification).
        Family family = new Family();
        family.setLastName(documentLastName);
        family.setId(documentId);
        family.setDistrict("Columbia"); // Document modification

        Mono<CosmosItemResponse<Family>> itemResponseMono =
            container.upsertItem(family, new CosmosItemRequestOptions());

        itemResponseMono.flatMap(itemResponse -> {
            CosmosDiagnostics diagnostics = itemResponse.getDiagnostics();
            logger.info("Upsert item diagnostics : {}", diagnostics);
            return Mono.just(itemResponse);
        }).block();

        logger.info("Done.");
    }

    // Document delete
    private void deleteDocument() throws Exception {
        logger.info("Delete document " + documentId + " by ID.");

        // Delete document
        Mono<CosmosItemResponse<Object>> itemResponseMono = container.deleteItem(documentId,
            new PartitionKey(documentLastName), new CosmosItemRequestOptions());

        itemResponseMono.flatMap(itemResponse -> {
            CosmosDiagnostics diagnostics = itemResponse.getDiagnostics();
            logger.info("Delete item diagnostics : {}", diagnostics);
            return Mono.just(itemResponse);
        }).block();

        logger.info("Done.");
    }

    // Database delete
    private void deleteDatabase() throws Exception {
        logger.info("Last step: delete database " + databaseName + " by ID.");

        // Delete database
        CosmosDatabaseResponse dbResp =
            client.getDatabase(databaseName).delete(new CosmosDatabaseRequestOptions()).block();
        logger.info("Status code for database delete: {}", dbResp.getStatusCode());

        logger.info("Done.");
    }

    // Cleanup before close
    private void shutdown() {
        try {
            //Clean shutdown
            deleteDocument();
            deleteDatabase();
        } catch (Exception err) {
            logger.error("Deleting Cosmos DB resources failed, will still attempt to close the client. See stack "
                + "trace below.");
            err.printStackTrace();
        }
        client.close();
        logger.info("Done with sample.");
    }
}
