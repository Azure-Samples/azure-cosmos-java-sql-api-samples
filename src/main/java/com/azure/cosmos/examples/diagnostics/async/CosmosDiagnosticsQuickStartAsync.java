// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.diagnostics.async;

import com.azure.core.util.Context;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDiagnostics;
import com.azure.cosmos.CosmosDiagnosticsContext;
import com.azure.cosmos.CosmosDiagnosticsHandler;
import com.azure.cosmos.CosmosDiagnosticsThresholds;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.models.CosmosClientTelemetryConfig;
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
import reactor.core.publisher.Mono;

import java.time.Duration;
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

        logger.info("Using Azure Cosmos DB endpoint: {}", AccountSettings.HOST);

        //  Create diagnostics threshold
        CosmosDiagnosticsThresholds cosmosDiagnosticsThresholds = new CosmosDiagnosticsThresholds();
        //  These thresholds are for demo purposes
        //  NOTE: Do not use the same thresholds for production
        cosmosDiagnosticsThresholds.setPayloadSizeThreshold(100_00);
        cosmosDiagnosticsThresholds.setPointOperationLatencyThreshold(Duration.ofSeconds(1));
        cosmosDiagnosticsThresholds.setNonPointOperationLatencyThreshold(Duration.ofSeconds(5));
        cosmosDiagnosticsThresholds.setRequestChargeThreshold(100f);

        //  By default, DEFAULT_LOGGING_HANDLER can be used
        CosmosDiagnosticsHandler cosmosDiagnosticsHandler = CosmosDiagnosticsHandler.DEFAULT_LOGGING_HANDLER;

        //  App developers can also define their own diagnostics handler
        cosmosDiagnosticsHandler = new CosmosDiagnosticsHandler() {
            @Override
            public void handleDiagnostics(CosmosDiagnosticsContext diagnosticsContext, Context traceContext) {
                logger.info("This is custom diagnostics handler: {}", diagnosticsContext.toJson());
            }
        };


        //  Create Client Telemetry Config
        CosmosClientTelemetryConfig cosmosClientTelemetryConfig =
            new CosmosClientTelemetryConfig();
        cosmosClientTelemetryConfig.diagnosticsHandler(cosmosDiagnosticsHandler);
        cosmosClientTelemetryConfig.diagnosticsThresholds(cosmosDiagnosticsThresholds);

        //  Create async client
        client = new CosmosClientBuilder()
            .endpoint(AccountSettings.HOST)
            .key(AccountSettings.MASTER_KEY)
            .consistencyLevel(ConsistencyLevel.EVENTUAL)
            .contentResponseOnWriteEnabled(true)
            .clientTelemetryConfig(cosmosClientTelemetryConfig)
            .buildAsyncClient();


        createDatabaseIfNotExists();
        createContainerIfNotExists();

        createDocument();
        readDocumentById();
        readDocumentDoesntExist();
        queryDocuments();
        replaceDocument();
        upsertDocument();
    }

    // Database Diagnostics
    private void createDatabaseIfNotExists() throws Exception {
        logger.info("Creating database {} if not exists", databaseName);

        //  Create database if not exists
        Mono<CosmosDatabaseResponse> databaseResponseMono = client.createDatabaseIfNotExists(databaseName);
        CosmosDatabaseResponse cosmosDatabaseResponse = databaseResponseMono.block();

        CosmosDiagnostics diagnostics = cosmosDatabaseResponse.getDiagnostics();
        logger.info("Create database diagnostics : {}", diagnostics);

        database = client.getDatabase(cosmosDatabaseResponse.getProperties().getId());

        logger.info("Done.");
    }

    // Container create
    private void createContainerIfNotExists() throws Exception {
        logger.info("Creating container {} if not exists", containerName);

        //  Create container if not exists
        CosmosContainerProperties containerProperties =
            new CosmosContainerProperties(containerName, "/lastName");

        // Provision throughput
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);

        //  Create container with 200 RU/s
        Mono<CosmosContainerResponse> containerResponseMono = database.createContainerIfNotExists(containerProperties,
            throughputProperties);
        CosmosContainerResponse cosmosContainerResponse = containerResponseMono.block();
        CosmosDiagnostics diagnostics = cosmosContainerResponse.getDiagnostics();
        logger.info("Create container diagnostics : {}", diagnostics);
        container = database.getContainer(cosmosContainerResponse.getProperties().getId());

        logger.info("Done.");
    }

    private void createDocument() throws Exception {
        logger.info("Create document : {}", documentId);

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

        CosmosItemResponse<Family> itemResponse = itemResponseMono.block();
        CosmosDiagnostics diagnostics = itemResponse.getDiagnostics();
        logger.info("Create item diagnostics : {}", diagnostics);

        logger.info("Done.");
    }

    // Document read
    private void readDocumentById() throws Exception {
        logger.info("Read document by ID : {}", documentId);

        //  Read document by ID
        Mono<CosmosItemResponse<Family>> itemResponseMono = container.readItem(documentId,
            new PartitionKey(documentLastName), Family.class);

        CosmosItemResponse<Family> familyCosmosItemResponse = itemResponseMono.block();
        CosmosDiagnostics diagnostics = familyCosmosItemResponse.getDiagnostics();
        logger.info("Read item diagnostics : {}", diagnostics);

        Family family = familyCosmosItemResponse.getItem();

        // Check result
        logger.info("Finished reading family {} with partition key {}", family.getId(), family.getLastName());

        logger.info("Done.");
    }

    // Document read doesn't exist
    private void readDocumentDoesntExist() throws Exception {
        logger.info("Read document by ID : bad-ID");

        //  Read document by ID
        try {
            CosmosItemResponse<Family> familyCosmosItemResponse = container.readItem("bad-ID",
                new PartitionKey("bad-lastName"), Family.class).block();
        } catch (CosmosException cosmosException) {
            CosmosDiagnostics diagnostics = cosmosException.getDiagnostics();
            logger.info("Read item exception diagnostics : {}", diagnostics);
        }

        logger.info("Done.");
    }

    private void queryDocuments() throws Exception {
        logger.info("Query documents in the container : {}", containerName);

        String sql = "SELECT * FROM c WHERE c.lastName = 'Witherspoon'";

        CosmosPagedFlux<Family> filteredFamilies = container.queryItems(sql, new CosmosQueryRequestOptions(),
            Family.class);

        //  Add handler to capture diagnostics
        filteredFamilies = filteredFamilies.handle(familyFeedResponse -> {
            logger.info("Query Item diagnostics through handler : {}", familyFeedResponse.getCosmosDiagnostics());
        });

        //  Or capture diagnostics through byPage() APIs.
        filteredFamilies.byPage().toIterable().forEach(familyFeedResponse -> {
            logger.info("Query item diagnostics through iterableByPage : {}",
                familyFeedResponse.getCosmosDiagnostics());
        });

        logger.info("Done.");
    }

    private void replaceDocument() throws Exception {
        logger.info("Replace document : {}", documentId);

        // Replace existing document with new modified document
        Family family = new Family();
        family.setLastName(documentLastName);
        family.setId(documentId);
        family.setDistrict("Columbia"); // Document modification

        Mono<CosmosItemResponse<Family>> itemResponseMono =
            container.replaceItem(family, family.getId(), new PartitionKey(family.getLastName()),
                new CosmosItemRequestOptions());

        CosmosItemResponse<Family> itemResponse = itemResponseMono.block();
        CosmosDiagnostics diagnostics = itemResponse.getDiagnostics();
        logger.info("Replace item diagnostics : {}", diagnostics);
        logger.info("Request charge of replace operation: {} RU", itemResponse.getRequestCharge());

        logger.info("Done.");
    }

    private void upsertDocument() throws Exception {
        logger.info("Replace document : {}", documentId);

        // Replace existing document with new modified document (contingent on modification).
        Family family = new Family();
        family.setLastName(documentLastName);
        family.setId(documentId);
        family.setDistrict("Columbia"); // Document modification

        Mono<CosmosItemResponse<Family>> itemResponseMono =
            container.upsertItem(family, new CosmosItemRequestOptions());

        CosmosItemResponse<Family> itemResponse = itemResponseMono.block();
        CosmosDiagnostics diagnostics = itemResponse.getDiagnostics();
        logger.info("Upsert item diagnostics : {}", diagnostics);

        logger.info("Done.");
    }

    // Document delete
    private void deleteDocument() throws Exception {
        logger.info("Delete document by ID {}", documentId);

        // Delete document
        Mono<CosmosItemResponse<Object>> itemResponseMono = container.deleteItem(documentId,
            new PartitionKey(documentLastName), new CosmosItemRequestOptions());

        CosmosItemResponse<Object> itemResponse = itemResponseMono.block();
        CosmosDiagnostics diagnostics = itemResponse.getDiagnostics();
        logger.info("Delete item diagnostics : {}", diagnostics);

        logger.info("Done.");
    }

    // Database delete
    private void deleteDatabase() throws Exception {
        logger.info("Last step: delete database {} by ID", databaseName);

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
            logger.error("Deleting Cosmos DB resources failed, will still attempt to close the client", err);
        }
        client.close();
        logger.info("Done with sample.");
    }
}
