// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.prometheus.async;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDiagnostics;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.models.CosmosClientTelemetryConfig;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseRequestOptions;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosMetricTagName;
import com.azure.cosmos.models.CosmosMicrometerMetricsOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.test.faultinjection.CosmosFaultInjectionHelper;
import com.azure.cosmos.test.faultinjection.FaultInjectionConditionBuilder;
import com.azure.cosmos.test.faultinjection.FaultInjectionOperationType;
import com.azure.cosmos.test.faultinjection.FaultInjectionResultBuilders;
import com.azure.cosmos.test.faultinjection.FaultInjectionRule;
import com.azure.cosmos.test.faultinjection.FaultInjectionRuleBuilder;
import com.azure.cosmos.test.faultinjection.FaultInjectionServerErrorType;
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
import java.util.Arrays;
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
    public static final int NUMBER_OF_DOCS = 1000;

    public ArrayList<JsonNode> docs;
    private final static Logger logger = LoggerFactory.getLogger(CosmosClientMetricsQuickStartAsync.class);

    public void close() {
        client.close();
    }

    public static void main(String[] args)  {
        CosmosClientMetricsQuickStartAsync quickStart = new CosmosClientMetricsQuickStartAsync();

        try {
            logger.info("Starting ASYNC main");
            quickStart.clientMetricsDemo();
            logger.info("Demo complete, please hold while resources are released");
        }  finally {
            logger.info("Shutting down");
            quickStart.shutdown();
        }
    }

    private void clientMetricsDemo()  {

        logger.info("Using Azure Cosmos DB endpoint: {}", AccountSettings.HOST);

        FaultInjectionRule serverConnectionDelayRule =
                new FaultInjectionRuleBuilder("ServerError-ConnectionTimeout")
                        .condition(
                                new FaultInjectionConditionBuilder()
                                        .operationType(FaultInjectionOperationType.CREATE_ITEM)
                                        .build()
                        )
                        .result(
                                FaultInjectionResultBuilders
                                        .getResultBuilder(FaultInjectionServerErrorType.RESPONSE_DELAY)
                                        .delay(Duration.ofSeconds(6)) // default connection timeout is 5s
                                        .times(1)
                                        .build()
                        )
                        .duration(Duration.ofMillis(500))
                        .hitLimit(10)
                        .build();

        FaultInjectionRule partitionSplitRule =
                new FaultInjectionRuleBuilder("Partition-Split")
                        .condition(
                                new FaultInjectionConditionBuilder()
                                        .operationType(FaultInjectionOperationType.CREATE_ITEM)
                                        .build()
                        )
                        .result(
                                FaultInjectionResultBuilders
                                        .getResultBuilder(FaultInjectionServerErrorType.GONE)
                                        .times(1)
                                        .build()
                        )
                        .duration(Duration.ofMillis(2000))
                        .hitLimit(50)
                        .build();

        FaultInjectionRule readTimeouts =
                new FaultInjectionRuleBuilder("Read-Timeouts")
                        .condition(
                                new FaultInjectionConditionBuilder()
                                        .operationType(FaultInjectionOperationType.READ_ITEM)
                                        .build()
                        )
                        .result(
                                FaultInjectionResultBuilders
                                        .getResultBuilder(FaultInjectionServerErrorType.TIMEOUT)
                                        .times(1)
                                        .build()
                        )
                        .duration(Duration.ofMillis(2000))
                        .hitLimit(50)
                        .build();


        //prometheus meter registry
        PrometheusMeterRegistry prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        //provide the prometheus registry to the telemetry config
        CosmosClientTelemetryConfig telemetryConfig = new CosmosClientTelemetryConfig()
                .clientCorrelationId("samplePrometheusMetrics001")
                .metricsOptions(new CosmosMicrometerMetricsOptions().meterRegistry(prometheusRegistry)
                        .configureDefaultTagNames(CosmosMetricTagName.PARTITION_KEY_RANGE_ID));

        //start local HttpServer server to expose the meter registry metrics to Prometheus
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
            .consistencyLevel(ConsistencyLevel.SESSION) //make sure we can read our own writes
            .contentResponseOnWriteEnabled(true)

            .buildAsyncClient();

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

        // inject fault injection rules
        CosmosFaultInjectionHelper.configureFaultInjectionRules(container, Arrays.asList(serverConnectionDelayRule)).block();

        docs = generateDocs(NUMBER_OF_DOCS);
        createManyDocuments();
        readManyDocuments();
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

        //  Create container with 200 RU/s
        Mono<CosmosContainerResponse> containerResponseMono = database.createContainerIfNotExists(containerProperties,
            throughputProperties);
        CosmosContainerResponse cosmosContainerResponse = containerResponseMono.block();
        CosmosDiagnostics diagnostics = cosmosContainerResponse.getDiagnostics();
        logger.info("Create container diagnostics : {}", diagnostics);
        container = database.getContainer(cosmosContainerResponse.getProperties().getId());

        logger.info("Done.");
    }

    private void createManyDocuments()  {
        Flux.fromIterable(docs).flatMap(doc -> container.createItem(doc))
                .flatMap(itemResponse -> {
                    if (itemResponse.getStatusCode() == 201) {
                        number_docs_inserted.getAndIncrement();
                    } else
                        logger.info("WARNING insert status code {} != 201" + itemResponse.getStatusCode());
                    write_request_count.incrementAndGet();
                    return Mono.empty();
                })
                .onErrorContinue((throwable, o) -> {
                    logger.error(
                            "Exception in create docs. e: {}", throwable.getMessage(), throwable
                            );
                }).blockLast();
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
                        logger.info("read item with id: " + itemResponse.getItem().get("id"));
                    } else
                        logger.info("WARNING insert status code {} != 200" + itemResponse.getStatusCode());
                    read_request_count.getAndIncrement();
                    return Mono.empty();
                })
                .onErrorContinue((throwable, o) -> {
                    logger.error(
                            "Exception in create docs. e: {}", throwable.getMessage(), throwable
                    );
                }).blockLast();
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
            //deleteDatabase();
        } catch (Exception err) {
            logger.error("Deleting Cosmos DB resources failed, will still attempt to close the client", err);
        }
        //client.close();
        logger.info("Done with sample.");
    }
}
