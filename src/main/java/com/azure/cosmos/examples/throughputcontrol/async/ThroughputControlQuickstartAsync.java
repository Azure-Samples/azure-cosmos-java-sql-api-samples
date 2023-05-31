// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.throughputcontrol.async;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.GlobalThroughputControlConfig;
import com.azure.cosmos.ThrottlingRetryOptions;
import com.azure.cosmos.ThroughputControlGroupConfig;
import com.azure.cosmos.ThroughputControlGroupConfigBuilder;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PriorityLevel;
import com.azure.cosmos.models.ThroughputProperties;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.azure.cosmos.examples.common.Profile.generateDocs;

public class ThroughputControlQuickstartAsync {
    private static CosmosAsyncClient client1;
    private static CosmosAsyncClient client2;
    int random_int = (int) Math.floor(Math.random() * (100 - 50 + 1) + 50);
    private final String databaseName = "ThroughputControlDemoDB" + random_int;
    private final String throughputControlContainerName = "ThroughputControl";
    private final String priorityBasedThrottlingContainerName = "priorityBasedThrottlingContainer";
    private final String containerName = "CosmosContainer";
    private CosmosAsyncDatabase database1;
    private CosmosAsyncDatabase database2;
    private CosmosAsyncContainer ThroughputControlTestContainerObject1;
    private CosmosAsyncContainer ThroughputControlTestContainerObject2;
    private CosmosAsyncContainer priorityBasedThrottlingContainerObject1;
    private CosmosAsyncContainer priorityBasedThrottlingContainerObject2;
    private static AtomicInteger request_count = new AtomicInteger(1);
    private static AtomicInteger rate_limit_error_count = new AtomicInteger(0);
    //set max number of retries for 429 (throttling) to low number so that we see rate limiting errors
    ThrottlingRetryOptions retryOptions = new ThrottlingRetryOptions().setMaxRetryAttemptsOnThrottledRequests(1);
    public static final int PROVISIONED_RUS = 10000;
    public static final int THROUGHPUT_CONTROL_RUS = 200;
    public static final int NUMBER_OF_DOCS = 2000;
    public static final int NUMBER_OF_DOCS_PRIORITY_BASED_THROTTLING = 100;

    public ArrayList<JsonNode> docs;
    CosmosItemRequestOptions options = new CosmosItemRequestOptions();
    private final static Logger logger = LoggerFactory.getLogger(ThroughputControlQuickstartAsync.class);
    public void closeClient1() {
        client1.close();
    }
    public void closeClient2() {
        client2.close();
    }

    /**
     * Demo throughput control...
     */
    // <Main>
    public static void main(String[] args) {
        ThroughputControlQuickstartAsync p = new ThroughputControlQuickstartAsync();

        try {
            p.throughControlDemo();
            logger.info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            logger.info("Cosmos getStarted failed with: " + e);
        } finally {
            logger.info("Closing clients");
            p.closeClient1();
            p.closeClient2();
        }
        System.exit(0);
    }
    // </Main>

    private void throughControlDemo() throws Exception {
        logger.info("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        client1 = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .contentResponseOnWriteEnabled(true)
                .directMode()
                .throttlingRetryOptions(retryOptions)
                .buildAsyncClient();

        client2 = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .contentResponseOnWriteEnabled(true)
                .directMode()
                .throttlingRetryOptions(retryOptions)
                .buildAsyncClient();

        Mono<Void> databaseContainerIfNotExist = client1.createDatabaseIfNotExists(databaseName)
                .flatMap(databaseResponse -> {
                    database1 = client1.getDatabase(databaseResponse.getProperties().getId());
                    logger.info("Created database" + databaseName);
                    CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, "/id");
                    ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(PROVISIONED_RUS);
                    return database1.createContainerIfNotExists(containerProperties, throughputProperties);
                }).flatMap(containerResponse -> {
                    ThroughputControlTestContainerObject1 = database1.getContainer(containerResponse.getProperties().getId());
                    logger.info("Created container " + containerName);
                    return Mono.empty();
                });

        logger.info("Creating database and container asynchronously...");
        databaseContainerIfNotExist.block();

        logger.info("Creating throughput control container to manage global throughput control metadata...");
        //<CreateThroughputControlContainer>
        database1 = client1.getDatabase(databaseName);
        //NOTE: this container is not subject to throughput control, as it is used to manage throughput control
        //NOTE: throughput control container MUST be created with partition key of /groupId and TTL must be set.
        CosmosContainerProperties throughputContainerProperties = new CosmosContainerProperties(throughputControlContainerName, "/groupId").setDefaultTimeToLiveInSeconds(-1);
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(PROVISIONED_RUS);
        database1.createContainerIfNotExists(throughputContainerProperties, throughputProperties).block();
        //</CreateThroughputControlContainer>

        logger.info("Creating container for priority based throttling test, with only 400 RUs...");
        CosmosContainerProperties priorityBasedThrottlingContainerProperties = new CosmosContainerProperties(priorityBasedThrottlingContainerName, "/id").setDefaultTimeToLiveInSeconds(-1);
        ThroughputProperties priorityBasedThrottlingContainerThroughputProperties = ThroughputProperties.createManualThroughput(400);
        database1.createContainerIfNotExists(priorityBasedThrottlingContainerProperties, priorityBasedThrottlingContainerThroughputProperties).block();

        logger.info("Running a base test which should not produce rate limiting...");
        baseTest();
        logger.info("Running a local throughput control test with 2 clients loading " + NUMBER_OF_DOCS + " docs and having " + THROUGHPUT_CONTROL_RUS + " RU limit each...");
        localThroughputControlTest();
        logger.info("Running a global throughput control test with 2 clients loading " + NUMBER_OF_DOCS + " docs and sharing " + THROUGHPUT_CONTROL_RUS + " RU limit...");
        //this should produce more rate limiting than the local throughput control above, as both clients share the same RU limit in the group
        //search for isThroughputControlRequestRateTooLarge in the logs to see the rate limiting from throughput control.
        globalThroughputControlTest();
        logger.info("Running priority based throttling test with two clients loading " + NUMBER_OF_DOCS_PRIORITY_BASED_THROTTLING + " docs");
        //one client has priority over the other, so it should be throttled less
        priorityBasedThrottling();
        database1.delete().block();
    }

    private void baseTest() {
        try {
            docs = generateDocs(NUMBER_OF_DOCS);
            createManyItems("BASE TEST", docs, options);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Local throughput control test - we will create a local throughput control group
     * for each client, and each client will have a limit of 200 RU/s
     */
    private void localThroughputControlTest() {
        database2 = client2.getDatabase(databaseName);
        ThroughputControlTestContainerObject2 = database2.getContainer(containerName);
        options.setThroughputControlGroupName("localControlGroup");
        ThroughputControlGroupConfig groupConfig1 =
                new ThroughputControlGroupConfigBuilder()
                        .groupName("localControlGroup")
                        // use below to set a target throughput threshold as % of provisioned throughput instead of absolute value
                        //.targetThroughputThreshold(0.25)
                        .targetThroughput(THROUGHPUT_CONTROL_RUS)
                        .build();
        ThroughputControlTestContainerObject1.enableLocalThroughputControlGroup(groupConfig1);
        ThroughputControlGroupConfig groupConfig2 =
                new ThroughputControlGroupConfigBuilder()
                        .groupName("localControlGroup")
                        // use below to set a target throughput threshold as % of provisioned throughput instead of absolute value
                        //.targetThroughputThreshold(0.25)
                        .targetThroughput(THROUGHPUT_CONTROL_RUS)
                        .build();
        ThroughputControlTestContainerObject2.enableLocalThroughputControlGroup(groupConfig2);
        try {
            //createManyItems("loacl throughput test", docs, options);
            createManyItemsWithTwoClients(NUMBER_OF_DOCS, "LOCAL THROUGHPUT CONTROL TEST", options, Arrays.asList(ThroughputControlTestContainerObject1, ThroughputControlTestContainerObject2));
        } catch (Exception e) {
            logger.info("Exception in localThroughputControlTest: " + e);
        }
    }

    /**
     * Global throughput control test - we will create a global throughput control group
     * that will be shared by both clients - they will share a limit of 200 RU/s
     */
    private void globalThroughputControlTest() {
        database2 = client2.getDatabase(databaseName);
        ThroughputControlTestContainerObject2 = database2.getContainer(containerName);
        CosmosItemRequestOptions options = new CosmosItemRequestOptions();
        options.setThroughputControlGroupName("globalControlGroup");
        ThroughputControlGroupConfig groupConfig =
                new ThroughputControlGroupConfigBuilder()
                        .groupName("globalControlGroup")
                        // use below to set a target throughput threshold as % of provisioned throughput instead of absolute value
                        //.targetThroughputThreshold(0.25)
                        .targetThroughput(THROUGHPUT_CONTROL_RUS)
                        .build();
        GlobalThroughputControlConfig globalControlConfig1 =
                this.client1.createGlobalThroughputControlConfigBuilder(database1.getId(), throughputControlContainerName)
                        .setControlItemRenewInterval(Duration.ofSeconds(5))
                        .setControlItemExpireInterval(Duration.ofSeconds(20))
                        .build();
        GlobalThroughputControlConfig globalControlConfig2 =
                this.client2.createGlobalThroughputControlConfigBuilder(database1.getId(), throughputControlContainerName)
                        .setControlItemRenewInterval(Duration.ofSeconds(5))
                        .setControlItemExpireInterval(Duration.ofSeconds(20))
                        .build();
        ThroughputControlTestContainerObject1.enableGlobalThroughputControlGroup(groupConfig, globalControlConfig1);
        ThroughputControlTestContainerObject2.enableGlobalThroughputControlGroup(groupConfig, globalControlConfig2);
        try {
            createManyItemsWithTwoClients(NUMBER_OF_DOCS, "GLOBAL THROUGHPUT CONTROL TEST", options, Arrays.asList(ThroughputControlTestContainerObject1, ThroughputControlTestContainerObject2));
        } catch (Exception e) {
            logger.info("Exception in globalThroughputControlTest: " + e);
        }
    }

    /**
     * Priority based throttling test - one client will have priority of LOW, the other HIGH
     */
    private void priorityBasedThrottling() {
        database1 = client1.getDatabase(databaseName);
        database2 = client2.getDatabase(databaseName);
        priorityBasedThrottlingContainerObject1 = database1.getContainer(priorityBasedThrottlingContainerName);
        priorityBasedThrottlingContainerObject2 = database2.getContainer(priorityBasedThrottlingContainerName);
        CosmosItemRequestOptions options = new CosmosItemRequestOptions();
        options.setThroughputControlGroupName("priorityBasedThrottling");
        ThroughputControlGroupConfig groupConfig1 =
                new ThroughputControlGroupConfigBuilder()
                        .groupName("priorityBasedThrottling")
                        .priorityLevel(PriorityLevel.HIGH)
                        .build();
        ThroughputControlGroupConfig groupConfig2 =
                new ThroughputControlGroupConfigBuilder()
                        .groupName("priorityBasedThrottling")
                        .priorityLevel(PriorityLevel.LOW)
                        .build();
        ThroughputControlTestContainerObject1.enableLocalThroughputControlGroup(groupConfig1);
        ThroughputControlTestContainerObject2.enableLocalThroughputControlGroup(groupConfig2);
        try {
            createManyItemsWithTwoClients(NUMBER_OF_DOCS_PRIORITY_BASED_THROTTLING, "PRIORITY BASED THROTTLING TEST", options, Arrays.asList(priorityBasedThrottlingContainerObject1, priorityBasedThrottlingContainerObject2));
            Thread.sleep(2000);
        } catch (Exception e) {
            logger.info("Exception in globalThroughputControlTest: " + e);
        }

    }


    private void createManyItems(String test, ArrayList<JsonNode> docs, CosmosItemRequestOptions options) throws Exception {
        Flux.fromIterable(docs).flatMap(doc -> ThroughputControlTestContainerObject1.createItem(doc, options)).flatMap(itemResponse -> {
                    if (itemResponse.getStatusCode() == 201) {
                        request_count.incrementAndGet();
                    } else {
                        logger.info("WARNING insert status code {} != 201" + itemResponse.getStatusCode());
                        request_count.incrementAndGet();
                    }
                    return Mono.empty();
                }).doOnError((exception) -> {
                    request_count.incrementAndGet();
                    rate_limit_error_count.incrementAndGet();
                    logger.info(
                            "error creating item in " + test + " e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                })
                .blockLast(); // block to wait for all items to be created
        logger.info("total request count was: " + request_count.get());
        logger.info("\n\n\n****************\n***TOTAL NUMBER OF RATE LIMIT ERRORS RECORDED IN " + test + " was: " + rate_limit_error_count.get()+"\n***************\n\n\n");
        request_count.set(0);
        rate_limit_error_count.set(0);
    }

    private void createManyItemsWithTwoClients(int noOfDocs, String test, CosmosItemRequestOptions options, List<CosmosAsyncContainer> containers) throws Exception {
        int clientId = 1;
        for (CosmosAsyncContainer cosmosAsyncContainer : containers) {
            logger.info("client " + clientId + " of " + test);
            docs = generateDocs(noOfDocs);
            int finalClientId = clientId;
            Flux.fromIterable(docs).flatMap(doc -> cosmosAsyncContainer.createItem(doc, options)).flatMap(itemResponse -> {
                if (itemResponse.getStatusCode() == 201) {
                    //uncomment below to see diagnostics in logs showing retries and isThroughputControlRequestRateTooLarge value
                    //logger.info("printing diagnostics to see retries isThroughputControlRequestRateTooLarge value" + itemResponse.getDiagnostics());
                } else {
                    logger.info("WARNING insert status code {} != 201" + itemResponse.getStatusCode());
                }
                request_count.incrementAndGet();
                return Mono.empty();
            }).doOnError((exception) -> {
                request_count.incrementAndGet();
                rate_limit_error_count.incrementAndGet();
                logger.info(
                        "error creating item in " + test + " from client number " + finalClientId + " e: {}",
                        exception.getLocalizedMessage(),
                        exception);
            }).subscribe();
            clientId++;
        }
        int currentRequestCount = 0;
        //when request count stops increasing, we can assume that all requests and retries have been exhausted
        while (request_count.get() < noOfDocs * 2) {
            if (currentRequestCount == request_count.get()) {
                logger.info("request count has not increased in last 1 second, current request count is: " + request_count.get());
                break;
            }
            Thread.sleep(1000);
            currentRequestCount = request_count.get();
        }
        logger.info("total request count was: " + request_count.get());
        logger.info("\n\n\n****************\n***TOTAL NUMBER OF RATE LIMIT ERRORS RECORDED IN " + test + " was: " + rate_limit_error_count.get()+"\n***************\n\n\n");
        request_count.set(0);
        rate_limit_error_count.set(0);
    }

}
