// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.diagnostics.sync;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
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
import com.azure.cosmos.util.CosmosPagedIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class CosmosDiagnosticsQuickStart {

    private CosmosClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";
    private final String documentId = UUID.randomUUID().toString();
    private final String documentLastName = "Witherspoon";

    private CosmosDatabase database;
    private CosmosContainer container;

    private final static Logger logger = LoggerFactory.getLogger(CosmosDiagnosticsQuickStart.class);

    public void close() {
        client.close();
    }

    public static void main(String[] args) {
        CosmosDiagnosticsQuickStart quickStart = new CosmosDiagnosticsQuickStart();

        try {
            logger.info("Starting SYNC main");
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
            .buildClient();


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
        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName);
        CosmosDiagnostics diagnostics = databaseResponse.getDiagnostics();
        logger.info("Create database diagnostics : {}", diagnostics);
        database = client.getDatabase(databaseResponse.getProperties().getId());

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
        CosmosContainerResponse containerResponse = database.createContainerIfNotExists(containerProperties,
            throughputProperties);
        CosmosDiagnostics diagnostics = containerResponse.getDiagnostics();
        logger.info("Create container diagnostics : {}", diagnostics);
        container = database.getContainer(containerResponse.getProperties().getId());

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
        CosmosItemResponse<Family> item = container.createItem(family, new PartitionKey(family.getLastName()),
            new CosmosItemRequestOptions());

        CosmosDiagnostics diagnostics = item.getDiagnostics();
        logger.info("Create item diagnostics : {}", diagnostics);

        logger.info("Done.");
    }

    // Document read
    private void readDocumentById() throws Exception {
        logger.info("Read document " + documentId + " by ID.");

        //  Read document by ID
        CosmosItemResponse<Family> familyCosmosItemResponse = container.readItem(documentId,
            new PartitionKey(documentLastName), Family.class);

        CosmosDiagnostics diagnostics = familyCosmosItemResponse.getDiagnostics();
        logger.info("Read item diagnostics : {}", diagnostics);

        Family family = familyCosmosItemResponse.getItem();

        // Check result
        logger.info("Finished reading family " + family.getId() + " with partition key " + family.getLastName());

        logger.info("Done.");
    }

    private void queryDocuments() throws Exception {
        logger.info("Query documents in the container " + containerName + ".");

        String sql = "SELECT * FROM c WHERE c.lastName = 'Witherspoon'";

        CosmosPagedIterable<Family> filteredFamilies = container.queryItems(sql, new CosmosQueryRequestOptions(),
            Family.class);

        //  Add handler to capture diagnostics
        filteredFamilies = filteredFamilies.handle(familyFeedResponse -> {
            logger.info("Query Item diagnostics through handler : {}", familyFeedResponse.getCosmosDiagnostics());
        });

        //  Or capture diagnostics through iterableByPage() APIs.
        filteredFamilies.iterableByPage().forEach(familyFeedResponse -> {
            logger.info("Query item diagnostics through iterableByPage : {}",
                familyFeedResponse.getCosmosDiagnostics());
        });

        logger.info("Done.");
    }

    private void replaceDocument() throws Exception {
        logger.info("Replace document " + documentId);

        // Replace existing document with new modified document
        Family family = new Family();
        family.setLastName(documentLastName);
        family.setId(documentId);
        family.setDistrict("Columbia"); // Document modification

        CosmosItemResponse<Family> itemResponse =
            container.replaceItem(family, family.getId(), new PartitionKey(family.getLastName()),
                new CosmosItemRequestOptions());

        CosmosDiagnostics diagnostics = itemResponse.getDiagnostics();
        logger.info("Replace item diagnostics : {}", diagnostics);

        logger.info("Request charge of replace operation: {} RU", itemResponse.getRequestCharge());

        logger.info("Done.");
    }

    private void upsertDocument() throws Exception {
        logger.info("Replace document " + documentId);

        // Replace existing document with new modified document (contingent on modification).
        Family family = new Family();
        family.setLastName(documentLastName);
        family.setId(documentId);
        family.setDistrict("Columbia"); // Document modification

        CosmosItemResponse<Family> itemResponse =
            container.upsertItem(family, new CosmosItemRequestOptions());

        CosmosDiagnostics diagnostics = itemResponse.getDiagnostics();
        logger.info("Upsert item diagnostics : {}", diagnostics);

        logger.info("Done.");
    }

    // Document delete
    private void deleteDocument() throws Exception {
        logger.info("Delete document " + documentId + " by ID.");

        // Delete document
        CosmosItemResponse<Object> itemResponse = container.deleteItem(documentId,
            new PartitionKey(documentLastName), new CosmosItemRequestOptions());

        CosmosDiagnostics diagnostics = itemResponse.getDiagnostics();
        logger.info("Delete item diagnostics : {}", diagnostics);

        logger.info("Done.");
    }

    // Database delete
    private void deleteDatabase() throws Exception {
        logger.info("Last step: delete database " + databaseName + " by ID.");

        // Delete database
        CosmosDatabaseResponse dbResp = client.getDatabase(databaseName).delete(new CosmosDatabaseRequestOptions());
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
