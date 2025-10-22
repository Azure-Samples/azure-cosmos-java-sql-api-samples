package com.azure.cosmos.examples.opentelemetry;

import com.azure.cosmos.*;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.models.CosmosClientTelemetryConfig;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseRequestOptions;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Sample for configuring OpenTelemetry traces
 * 
 * OpenTelemetry data can be exported to the location of your choice. 
 * For example, follow these steps to configure sending OpenTelemetry traces and logs to Azure Monitor.
 * See the documentation for more information.
 * https://learn.microsoft.com/azure/azure-monitor/app/opentelemetry-enable?tabs=java
 * 
 *  1. Download the jar file https://github.com/microsoft/ApplicationInsights-Java/releases/download/3.4.12/applicationinsights-agent-3.4.12.jar.
 *  2. Point the JVM to the jar file by adding -javaagent:"path/to/applicationinsights-agent-3.4.12.jar" to your application's JVM args.
 *  3. Set an environment variable with your Application Insights connection string 
 *      - APPLICATIONINSIGHTS_CONNECTION_STRING=<Your Connection String>
 * 
 * In a high-volume, production environment you may want to enable sampling to reduce the amount of data emitted.
 */
public class SampleOpenTelemetryQuickStartAsync {

    private CosmosAsyncClient client;

    private final String databaseName = "samples";
    private final String containerName = "otel-sample";

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    protected static Logger logger = LoggerFactory.getLogger(SampleOpenTelemetryQuickStartAsync.class);

    public void close() {
        client.close();
    }

    /**
     * Sample to demonstrate distributed tracing with OpenTelemetry.
     */
    public static void main(String[] args) {
        SampleOpenTelemetryQuickStartAsync p = new SampleOpenTelemetryQuickStartAsync();
        try {
            logger.info("Starting ASYNC main");
            p.documentCRUDDemo();
            logger.info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Cosmos getStarted failed with {}", e);
        } finally {
            logger.info("Closing the client");
            p.shutdown();
        }
    }

    /**
     * @throws Exception
     */
    private void documentCRUDDemo() throws Exception {

        logger.info("Using Azure Cosmos DB endpoint: {}", AccountSettings.HOST);

        //  Create async client with diagnostics thresholds
        // <ConfigureDiagnosticsThresholds>
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .clientTelemetryConfig(new CosmosClientTelemetryConfig()
                    .diagnosticsThresholds(
                        new CosmosDiagnosticsThresholds()
                            .setPointOperationLatencyThreshold(Duration.ofMillis(100))
                            .setNonPointOperationLatencyThreshold(Duration.ofMillis(2000))
                            .setRequestChargeThreshold(100))
                )
                .contentResponseOnWriteEnabled(true)
                .buildAsyncClient();
        // </ConfigureDiagnosticsThresholds>

        createDatabaseIfNotExists();
        createContainerIfNotExists();
        
        int numDocs = 5;

        // CRUD workload to generate telemetry
        createDocuments(numDocs);
        // We are adding Thread.sleep to allow time for earlier processes to finish and send telemetry data.
        Thread.sleep(1000);

        readDocuments(numDocs);
        // We are adding Thread.sleep to allow time for earlier processes to finish and send telemetry data.
        Thread.sleep(1000);

        replaceDocuments(numDocs);
        // We are adding Thread.sleep to allow time for earlier processes to finish and send telemetry data.
        Thread.sleep(1000);

        deleteDocuments(numDocs);
        // We are adding Thread.sleep to allow time for earlier processes to finish and send telemetry data.
        Thread.sleep(5000);
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
        logger.info("Create container {} if not exists.", containerName);

        // Create container if not exists - this is async but we block to make sure
        // database and containers are created before sample runs the CRUD operations on
        // them
        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(containerName, "/lastName");

        // Provision throughput
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);

        //  Create container
        CosmosContainerResponse containerResponse = database.createContainerIfNotExists(containerProperties, throughputProperties).block();
        container = database.getContainer(containerResponse.getProperties().getId());

        logger.info("Done.");
    }

    private void createDocuments(int numDocs) throws Exception {
        for(int i = 1; i <= 5; i++){
            Family family = new Family();
            family.setLastName("Smith");
            family.setId(String.valueOf(i));

            // Insert this item as a document
            // Explicitly specifying the /pk value improves performance.
            // add subscribe() to make this async
            container.createItem(family, new PartitionKey(family.getLastName()), new CosmosItemRequestOptions())
                .doOnSuccess((response) -> {
                    logger.info("Inserted doc with id: {}", response.getItem().getId());
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).subscribe();
            logger.info("Creating doc asynchronously...");
        }
    }

    private void readDocuments(int numDocs) throws Exception {
        for(int i = 1; i <= 5; i++){
            //  Read document by ID
            container.readItem(String.valueOf(i), new PartitionKey("Smith"), Family.class)
                    .doOnSuccess((response) -> {
                        logger.info("Read doc with id {}", response.getItem().getId());
                    })
                    .doOnError(Exception.class, exception -> {
                        logger.error(
                                "Exception. e: {}",
                                exception.getLocalizedMessage(),
                                exception);
                    }).subscribe();

            logger.info("Reading documents by id asynchronously...");
        }
    }

    private void replaceDocuments(int numDocs) throws Exception {
        for(int i = 1; i <= 5; i++){
            // Replace existing document with new modified document
            Family family = new Family();
            family.setLastName("Smith");
            family.setId(String.valueOf(i));
            family.setDistrict("Columbia"); // Document modification

            container.replaceItem(family, family.getId(),
                            new PartitionKey(family.getLastName()), new CosmosItemRequestOptions())
                    .doOnSuccess(response -> {
                        logger.info("Replaced document with id {}", response.getItem().getId());
                    })
                    .doOnError((exception) -> {
                        logger.error(
                                "Exception. e: {}",
                                exception.getLocalizedMessage(),
                                exception);
                    }).subscribe();
            logger.info("Replacing documents asynchronously...");
        }
    }

    private void deleteDocuments(int numDocs) throws Exception {
        for(int i = 1; i <= 5; i++){
            // Delete document
            container.deleteItem(String.valueOf(i), new PartitionKey("Smith"), new CosmosItemRequestOptions())
                    .doOnSuccess(itemResponse -> {
                        logger.info("Deleted document.");
                    })
                    .doOnError((exception) -> {
                        logger.error(
                                "Exception. e: {}",
                                exception.getLocalizedMessage(),
                                exception);
                    }).subscribe();

            logger.info("Deleteing documents asynchronously...");
        }
    }
    
    private void deleteDatabase() throws Exception {
        logger.info("Last step: delete database {} by ID.", databaseName);

        // Delete database
        client.getDatabase(databaseName).delete(new CosmosDatabaseRequestOptions()).doOnSuccess(itemResponse -> {
                    logger.info("Request charge of delete database operation: {} RU", itemResponse.getRequestCharge());
                    logger.info("Status code for database delete: {}", itemResponse.getStatusCode());
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).block();

        logger.info("Done.");
    }

    // Cleanup before close
    private void shutdown() {
        try {
            //Clean shutdown
            deleteDatabase();
        } catch (Exception err) {
            logger.error("Deleting Cosmos DB resources failed, will still attempt to close the client. See stack trace below.");
            err.printStackTrace();
        }
        client.close();
        logger.info("Done with sample.");
    }
}
