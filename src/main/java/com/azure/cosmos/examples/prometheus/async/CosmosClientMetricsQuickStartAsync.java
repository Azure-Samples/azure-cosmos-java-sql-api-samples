// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.clientmetrics.async;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDiagnostics;
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
import com.azure.cosmos.models.CosmosMicrometerMetricsOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class CosmosClientMetricsQuickStartAsync {

    private CosmosAsyncClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";
    private final String documentId = UUID.randomUUID().toString();
    private final String documentLastName = "Witherspoon";

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    private static AtomicInteger number_docs_inserted = new AtomicInteger(0);
    private static AtomicInteger request_count = new AtomicInteger(0);
    public static final int NUMBER_OF_DOCS = 1000;

    public ArrayList<JsonNode> docs;
    private final static Logger logger = LoggerFactory.getLogger(CosmosClientMetricsQuickStartAsync.class);

    public void close() {
        client.close();
    }

    public static void main(String[] args) {
        CosmosClientMetricsQuickStartAsync quickStart = new CosmosClientMetricsQuickStartAsync();

        try {
            logger.info("Starting ASYNC main");
            quickStart.clientMetricsDemo();
            logger.info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            logger.error("Cosmos getStarted failed with", e);
        } finally {
            logger.info("Shutting down");
            quickStart.shutdown();
        }
    }

    private void clientMetricsDemo() throws Exception {

        logger.info("Using Azure Cosmos DB endpoint: {}", AccountSettings.HOST);


        PrometheusMeterRegistry prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        CosmosClientTelemetryConfig telemetryConfig = new CosmosClientTelemetryConfig()
                .metricsOptions(new CosmosMicrometerMetricsOptions().meterRegistry(prometheusRegistry));


        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
            server.createContext("/metrics", httpExchange -> {
                String response = prometheusRegistry.scrape();
                int i = 1;
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });

            new Thread(server::start).start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        //  Create sync client
        client = new CosmosClientBuilder()
            .endpoint(AccountSettings.HOST)
            .key(AccountSettings.MASTER_KEY)
            .clientTelemetryConfig(telemetryConfig)
            .consistencyLevel(ConsistencyLevel.EVENTUAL)
            .contentResponseOnWriteEnabled(true)
            .buildAsyncClient();


        createDatabaseIfNotExists();
        createContainerIfNotExists();

        createDocuments();
        readDocumentById();
        readDocumentDoesntExist();
        queryDocuments();
        replaceDocument();
        upsertDocument();
        pressAnyKeyToContinue("Press any key to continue ...");
    }

    private void pressAnyKeyToContinue(String message) {
        System.out.println(message);
        try {
            // noinspection ResultOfMethodCallIgnored
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

/*    private static MeterRegistry createConsoleLoggingMeterRegistry() {

        final MetricRegistry dropwizardRegistry = new MetricRegistry();

        final ConsoleReporter consoleReporter = ConsoleReporter
                .forRegistry(dropwizardRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        consoleReporter.start(1, TimeUnit.SECONDS);

        DropwizardConfig dropwizardConfig = new DropwizardConfig() {

            @Override
            public String get(@Nullable String key) {
                return null;
            }

            @Override
            public String prefix() {
                return "console";
            }

        };

        final DropwizardMeterRegistry consoleLoggingRegistry = new DropwizardMeterRegistry(
                dropwizardConfig, dropwizardRegistry, HierarchicalNameMapper.DEFAULT, Clock.SYSTEM) {
            @Override
            protected Double nullGaugeValue() {
                return Double.NaN;
            }

            @Override
            public void close() {
                super.close();
                consoleReporter.stop();
                consoleReporter.close();
            }
        };

        consoleLoggingRegistry.config().namingConvention(NamingConvention.dot);
        return consoleLoggingRegistry;
    }*/

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

    private void createDocuments() throws Exception {
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

    private void createManyDocuments(ArrayList<JsonNode> docs) throws Exception {
        docs = c
        final long startTime = System.currentTimeMillis();
        Flux.fromIterable(docs).flatMap(doc -> container.createItem(doc))
                .flatMap(itemResponse -> {
                    if (itemResponse.getStatusCode() == 201) {
                        number_docs_inserted.getAndIncrement();
                    } else
                        logger.info("WARNING insert status code {} != 201" + itemResponse.getStatusCode());
                    request_count.incrementAndGet();
                    return Mono.empty();
                }).subscribe(); // ...Subscribing to the publisher triggers stream execution.

        logger.info("Doing other things until async doc inserts complete...");
        while (request_count.get() < NUMBER_OF_DOCS) {
        }

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
        //client.close();
        logger.info("Done with sample.");
    }
}
