// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.prometheus.async;

import com.azure.cosmos.*;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.models.*;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.azure.cosmos.examples.common.Profile.generateDocs;

public class CosmosClientMetricsQuickStartAsync {

    private CosmosAsyncClient client;

    private final String databaseName = "ClientMetricsPrometheusDB";
    private final String containerName = "Container";

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    private static AtomicInteger number_docs_inserted = new AtomicInteger(0);
    private static AtomicInteger write_request_count = new AtomicInteger(0);
    private static AtomicInteger read_request_count = new AtomicInteger(0);
    public static final int NUMBER_OF_DOCS = 5000;

    public ArrayList<JsonNode> docs;
    private final static Logger logger = LoggerFactory.getLogger(CosmosClientMetricsQuickStartAsync.class);

    public void close() {
        client.close();
    }

    public static void main(String[] args)  {
        CosmosClientMetricsQuickStartAsync quickStart = new CosmosClientMetricsQuickStartAsync();

        try {
            logger.info("Starting ASYNC main");
            quickStart.clientMetricsPrometheusDemo();
            logger.info("Demo complete, please hold while resources are released");
        }  finally {
            logger.info("Shutting down");
            quickStart.shutdown();
        }
    }

    private void clientMetricsPrometheusDemo()  {

        logger.info("Using Azure Cosmos DB endpoint: {}", AccountSettings.HOST);


        // <ClientMetricsConfig>
        //prometheus meter registry
        PrometheusMeterRegistry prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        //provide the prometheus registry to the telemetry config
        CosmosClientTelemetryConfig telemetryConfig = new CosmosClientTelemetryConfig()
                .diagnosticsThresholds(
                        new CosmosDiagnosticsThresholds()
                                // Any requests that violate (are lower than) any of the below thresholds that are set
                                // will not appear in "request-level" metrics (those with "rntbd" or "gw" in their name).
                                // The "operation-level" metrics (those with "ops" in their name) will still be collected.
                                // Use this to reduce noise in the amount of metrics collected.
                                .setRequestChargeThreshold(10)
                                .setNonPointOperationLatencyThreshold(Duration.ofDays(10))
                                .setPointOperationLatencyThreshold(Duration.ofDays(10))
                )
                // Uncomment below to apply sampling to help further tune client-side resource consumption related to metrics.
                // The sampling rate can be modified after Azure Cosmos DB Client initialization – so the sampling rate can be
                // modified without any restarts being necessary.
                //.sampleDiagnostics(0.25)
                .clientCorrelationId("samplePrometheusMetrics001")
                .metricsOptions(new CosmosMicrometerMetricsOptions().meterRegistry(prometheusRegistry)
                        //.configureDefaultTagNames(CosmosMetricTagName.PARTITION_KEY_RANGE_ID)
                        .applyDiagnosticThresholdsForTransportLevelMeters(true)
                );
        // </ClientMetricsConfig>

        // Start local HttpServer server to expose the meter registry metrics to Prometheus.
        // When adding this endpoint to prometheus.yml, add the domain name and port to "targets".
        // For example, if prometheus is running on the same server as this app, you can add localhost:8080:
        // - targets: ["localhost:9090", "localhost:8080"]
        // download and install prometheus from here: https://prometheus.io/download/

        // <PrometheusTargetServer>
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
        // </PrometheusTargetServer>

        // <CosmosClient>
        //  Create async client
        client = new CosmosClientBuilder()
            .endpoint(AccountSettings.HOST)
            .key(AccountSettings.MASTER_KEY)
            .clientTelemetryConfig(telemetryConfig)
            .consistencyLevel(ConsistencyLevel.SESSION) //make sure we can read our own writes
            .contentResponseOnWriteEnabled(true)
            .buildAsyncClient();
        // </CosmosClient>

        try {
            createDatabaseIfNotExists();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            createContainerIfNotExists();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        docs = generateDocs(NUMBER_OF_DOCS);
        // None of the rntbd / request-level metrics for point operations will show as they violate one the thresholds set (minimum 10 RUs).
        createManyDocuments();
        readManyDocuments();
        // The rntbd / request-level metrics for the below query will show as it exceeds 10 RUs.
        // If you comment out the below, no rntbd / request-level metrics at all will be collected due to the thresholds set.
        queryAllDocuments();
    }


    // Database create
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
            new CosmosContainerProperties(containerName, "/id");

        // Provision throughput
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(10000);

        //  Create container
        Mono<CosmosContainerResponse> containerResponseMono = database.createContainerIfNotExists(containerProperties,
            throughputProperties);
        CosmosContainerResponse cosmosContainerResponse = containerResponseMono.block();
        CosmosDiagnostics diagnostics = cosmosContainerResponse.getDiagnostics();
        logger.info("Create container diagnostics : {}", diagnostics);
        container = database.getContainer(cosmosContainerResponse.getProperties().getId());

        logger.info("Done.");
    }

    private void createManyDocuments()  {
        Flux.fromIterable(docs).flatMap(doc -> container.createItem(doc)
                )
                .flatMap(itemResponse -> {
                    if (itemResponse.getStatusCode() == 201) {
                        number_docs_inserted.getAndIncrement();
                        write_request_count.incrementAndGet();
                    } else
                        logger.info("WARNING insert status code {} != 201" + itemResponse.getStatusCode());
                    return Mono.empty();
                })
                .onErrorContinue((throwable, o) -> {
                    logger.info(
                            "Exception in create docs. e: {}", throwable.getMessage(), throwable
                            );
                }).blockLast();
        logger.info("Number of successful write requests: " + write_request_count);
    }

    private void readManyDocuments()  {
        // collect the ids that were generated when writing the data.
        List<String> list = new ArrayList<String>();
        for (final JsonNode doc : docs) {
            list.add(doc.get("id").asText());
        }

        final long startTime = System.currentTimeMillis();
        Flux.fromIterable(list)
                .flatMap(id -> container.readItem(id, new PartitionKey(id), JsonNode.class))
                .flatMap(itemResponse -> {
                    if (itemResponse.getStatusCode() == 200) {
                        read_request_count.getAndIncrement();
                    } else
                        logger.info("WARNING insert status code {} != 200" + itemResponse.getStatusCode());
                    return Mono.empty();
                })
                .onErrorContinue((throwable, o) -> {
                    logger.info(
                            "Exception in create docs. e: {}", throwable.getMessage(), throwable
                    );
                }).blockLast();
        logger.info("Number of successful read requests: " + read_request_count);
    }

    private void queryAllDocuments()  {

        int preferredPageSize = number_docs_inserted.get(); // We'll use this later

        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();

        //  Set populate query metrics to get metrics around query executions
        queryOptions.setQueryMetricsEnabled(true);

        CosmosPagedFlux<Family> pagedFluxResponse = container.queryItems(
                "SELECT * FROM c", queryOptions, Family.class);

        try {

            pagedFluxResponse.byPage(preferredPageSize).flatMap(fluxResponse -> {
                logger.info("Got a page of query result with " +
                        fluxResponse.getResults().size() + " items(s)"
                        + " and request charge of " + fluxResponse.getRequestCharge());

                return Flux.empty();
            }).blockLast();

        } catch(Exception err) {
            if (err instanceof CosmosException) {
                //Client-specific errors
                CosmosException cerr = (CosmosException) err;
                cerr.printStackTrace();
                logger.error(String.format("Query failed with %s\n", cerr));
            } else {
                //General errors
                err.printStackTrace();
            }
        }
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
            deleteDatabase();
        } catch (Exception err) {
            logger.error("Deleting Cosmos DB resources failed, will still attempt to close the client", err);
        }
        //client.close();
        logger.info("Done with sample.");
    }
}
