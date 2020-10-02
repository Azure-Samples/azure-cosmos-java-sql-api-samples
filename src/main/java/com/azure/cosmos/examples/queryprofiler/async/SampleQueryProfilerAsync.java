package com.azure.cosmos.examples.queryprofiler.async;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.examples.common.Profile;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedFlux;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.stream.Collectors;


/*
 * Async Query Profiler Sample
 *
 * Please note that perf testing incurs costs for provisioning container throughput and storage.
 *
 * This query profiling sample issues a (user-defined) query and profiles the run-time to receive the full
 * set of response documents.
 *
 * Example configuration
 * -Provision X RU/s container throughput
 * -Replace default query string with custom query
 * -Result: Query takes T seconds
 */

public class SampleQueryProfilerAsync {

    protected static Logger logger = LoggerFactory.getLogger(SampleQueryProfilerAsync.class);

    public static void main(String[] args) {
        try {
            queryProfilerDemo();
        } catch(Exception err) {
            logger.error("Failed running demo: ", err);
        }
    }

    private static CosmosAsyncClient client;
    private static CosmosAsyncDatabase database;
    private static CosmosAsyncContainer container;
    private static String databaseName = "airlineTelemetry"; // Your Database name here
    private static String containerName = "airlineDemoDB"; // Your Container name here
    private static String partitionKey = "/partitionKey"; // Your partition key here
    private static int manualThroughput = 400; // Your manual throughput here
    private static String customQuery = "SELECT * FROM c WHERE c.reportId = 916779600";
//    private static String customQuery =
//        "SELECT * FROM c WHERE c.partitionKey ='Z50V4-745167' AND c.parameterDateTime >= '2020-04-10T27:16:00.000Z' AND c.parameterDateTime <= '2020-04-29T21:16:00.000Z' ORDER BY c.parameterDateTime DESC";

    public static void queryProfilerDemo() {

        // Create Async client.
        // Building an async client is still a sync operation.
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .contentResponseOnWriteEnabled(false)
                .buildAsyncClient();

        // Describe the logic of database and container creation using Reactor...
        client.createDatabaseIfNotExists(databaseName).flatMap(databaseResponse -> {
            database = client.getDatabase(databaseResponse.getProperties().getId());
            logger.info("\n\n\n\nCreated or connected to database {}.\n\n\n\n",databaseName);
            CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, partitionKey);
            ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(manualThroughput);
            return database.createContainerIfNotExists(containerProperties, throughputProperties);
        }).flatMap(containerResponse -> {
            container = database.getContainer(containerResponse.getProperties().getId());
            logger.info("\n\n\n\nCreated or connected to container {}.\n\n\n\n", containerName);
            return Mono.empty();
        }).block();

        // With the client set up we are ready to execute and profile our query.
        Profile.tic();

        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        queryOptions.setMaxDegreeOfParallelism(1000);
        queryOptions.setMaxBufferedItemCount(1000);
        int preferredPageSize = 1000;
        executeQuery(customQuery, queryOptions, preferredPageSize);

        double toc_time=Profile.toc_ms()/1000.0;
        logger.info("\n\n\n\nTotal query runtime (sec): {}.\n\n\n\n", toc_time);

        // Close client. This is always sync.
        logger.info("Closing client...");
        client.close();
        logger.info("Done with demo.");

    }

    private static void executeQuery(String query, CosmosQueryRequestOptions queryOptions, int preferredPageSize) {

        queryOptions.setQueryMetricsEnabled(false);

        CosmosPagedFlux<Family> pagedFluxResponse = container.queryItems(
                query, queryOptions, Family.class);

        try {

            double totalCharge = pagedFluxResponse.byPage(preferredPageSize).flatMap(fluxResponse -> {

                double requestCharge = fluxResponse.getRequestCharge();

                logger.info("Got a page of query result with " +
                        fluxResponse.getResults().size() + " items(s)"
                        + " and request charge of " + requestCharge);

                logger.info("Item Ids " + fluxResponse
                        .getResults()
                        .stream()
                        .map(Family::getId)
                        .collect(Collectors.toList()));

                return Mono.just(requestCharge);
            })
                    .reduce(0.0, (x1, x2) -> x1 + x2)
                    .block();

            logger.info("TOTAL QUERY CHARGE: {}", totalCharge);

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

}
