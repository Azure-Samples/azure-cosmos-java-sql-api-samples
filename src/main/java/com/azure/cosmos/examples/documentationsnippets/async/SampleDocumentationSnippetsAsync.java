// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.documentationsnippets.async;

import com.azure.core.http.ProxyOptions;
import com.azure.cosmos.ChangeFeedProcessor;
import com.azure.cosmos.ChangeFeedProcessorBuilder;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.DirectConnectionConfig;
import com.azure.cosmos.GatewayConnectionConfig;
import com.azure.cosmos.examples.common.CustomPOJO;
import com.azure.cosmos.examples.common.Families;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.models.ConflictResolutionPolicy;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosStoredProcedureProperties;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.ExcludedPath;
import com.azure.cosmos.models.IncludedPath;
import com.azure.cosmos.models.IndexingMode;
import com.azure.cosmos.models.IndexingPolicy;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SampleDocumentationSnippetsAsync {

    private CosmosAsyncClient client;

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    protected static Logger logger = LoggerFactory.getLogger(SampleDocumentationSnippetsAsync.class);

    /**
     * This file organizes Azure Docs Azure Cosmos DB async code snippets to enable easily upgrading to new Maven artifacts.
     * Usage: upgrade pom.xml to the latest Java SDK Maven artifact; rebuild this project; correct any public surface changes.
     * <p>
     * -
     */
    //  <Main>
    public static void main(String[] args) {
        // Do nothing. This file is meant to be built but not executed.
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/performance-tips-java-sdk-v4-sql
     * Performance tips - async Connection Mode
     */

    /** Performance tips - async Connection Mode */
    public static void PerformanceTipsJavaSDKv4ConnectionModeAsync() {

        String HOSTNAME = "";
        String MASTERKEY = "";
        ConsistencyLevel CONSISTENCY = ConsistencyLevel.EVENTUAL; //Arbitrary

        //  <PerformanceClientConnectionModeAsync>

        /* Direct mode, default settings */
        CosmosAsyncClient clientDirectDefault = new CosmosClientBuilder()
                .endpoint(HOSTNAME)
                .key(MASTERKEY)
                .consistencyLevel(CONSISTENCY)
                .directMode()
                .buildAsyncClient();

        /* Direct mode, custom settings */
        DirectConnectionConfig directConnectionConfig = DirectConnectionConfig.getDefaultConfig();

        // Example config, do not use these settings as defaults
        directConnectionConfig.setMaxConnectionsPerEndpoint(130);
        directConnectionConfig.setIdleConnectionTimeout(Duration.ZERO);

        CosmosAsyncClient clientDirectCustom = new CosmosClientBuilder()
                .endpoint(HOSTNAME)
                .key(MASTERKEY)
                .consistencyLevel(CONSISTENCY)
                .directMode(directConnectionConfig)
                .buildAsyncClient();

        /* Gateway mode, default settings */
        CosmosAsyncClient clientGatewayDefault = new CosmosClientBuilder()
                .endpoint(HOSTNAME)
                .key(MASTERKEY)
                .consistencyLevel(CONSISTENCY)
                .gatewayMode()
                .buildAsyncClient();

        /* Gateway mode, custom settings */
        GatewayConnectionConfig gatewayConnectionConfig = GatewayConnectionConfig.getDefaultConfig();

        // Example config, do not use these settings as defaults
        gatewayConnectionConfig.setProxy(new ProxyOptions(ProxyOptions.Type.HTTP, InetSocketAddress.createUnresolved("your.proxy.addr",80)));
        gatewayConnectionConfig.setMaxConnectionPoolSize(1000);

        CosmosAsyncClient clientGatewayCustom = new CosmosClientBuilder()
                .endpoint(HOSTNAME)
                .key(MASTERKEY)
                .consistencyLevel(CONSISTENCY)
                .gatewayMode(gatewayConnectionConfig)
                .buildAsyncClient();

        /* No connection mode, defaults to Direct mode with default settings */
        CosmosAsyncClient clientDefault = new CosmosClientBuilder()
                .endpoint(HOSTNAME)
                .key(MASTERKEY)
                .consistencyLevel(CONSISTENCY)
                .buildAsyncClient();

        //  </PerformanceClientConnectionModeAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/performance-tips-java-sdk-v4-sql
     * Performance tips - Direct data plane, Gateway control plane
     */

    /** Performance tips - Direct data plane, Gateway control plane */
    public static void PerformanceTipsJavaSDKv4CDirectOverrideAsync() {

        String HOSTNAME = "";
        String MASTERKEY = "";
        ConsistencyLevel CONSISTENCY = ConsistencyLevel.EVENTUAL; //Arbitrary

        //  <PerformanceClientDirectOverrideAsync>

        /* Independent customization of Direct mode data plane and Gateway mode control plane */
        DirectConnectionConfig directConnectionConfig = DirectConnectionConfig.getDefaultConfig();

        // Example config, do not use these settings as defaults
        directConnectionConfig.setMaxConnectionsPerEndpoint(130);
        directConnectionConfig.setIdleConnectionTimeout(Duration.ZERO);

        GatewayConnectionConfig gatewayConnectionConfig = GatewayConnectionConfig.getDefaultConfig();

        // Example config, do not use these settings as defaults
        gatewayConnectionConfig.setProxy(new ProxyOptions(ProxyOptions.Type.HTTP, InetSocketAddress.createUnresolved("your.proxy.addr",80)));
        gatewayConnectionConfig.setMaxConnectionPoolSize(1000);

        CosmosAsyncClient clientDirectCustom = new CosmosClientBuilder()
                .endpoint(HOSTNAME)
                .key(MASTERKEY)
                .consistencyLevel(CONSISTENCY)
                .directMode(directConnectionConfig,gatewayConnectionConfig)
                .buildAsyncClient();

        //  </PerformanceClientDirectOverrideAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/performance-tips-java-sdk-v4-sql
     * Performance tips - needs scheduler
     */

    /** Performance tips - needs scheduler */
    public static void PerformanceTipsJavaSDKv4ClientAsync() {

        String HOSTNAME = "";
        String MASTERKEY = "";
        ConsistencyLevel CONSISTENCY = ConsistencyLevel.EVENTUAL; //Arbitrary

        //  <PerformanceClientAsync>

        CosmosAsyncClient client = new CosmosClientBuilder()
                .endpoint(HOSTNAME)
                .key(MASTERKEY)
                .consistencyLevel(CONSISTENCY)
                .buildAsyncClient();

        //  </PerformanceClientAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/performance-tips-java-sdk-v4-sql
     * Performance tips - needs scheduler
     * Async only
     */

    /** Performance tips - needs scheduler */
    public static void PerformanceTipsJavaSDKv4NeedsSchedulerAsync() {

        CosmosAsyncContainer asyncContainer = null;
        CustomPOJO item = null;

        //  <PerformanceNeedsSchedulerAsync>

        Mono<CosmosItemResponse<CustomPOJO>> createItemPub = asyncContainer.createItem(item);
        createItemPub.subscribe(
                itemResponse -> {
                    //this is executed on eventloop IO netty thread.
                    //the eventloop thread is shared and is meant to return back quickly.
                    //
                    // DON'T do this on eventloop IO netty thread.
                    veryCpuIntensiveWork();
                });


        //  </PerformanceNeedsSchedulerAsync>
    }

    /** ^Dummy helper function for the above snippet  */
    public static void veryCpuIntensiveWork() {
        //Dummy
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/performance-tips-java-sdk-v4-sql
     * Performance tips - add scheduler
     * Async only
     */

    /** Performance tips - add scheduler */
    public static void PerformanceTipsJavaSDKv4AddSchedulerAsync() {

        CosmosAsyncContainer asyncContainer = null;
        CustomPOJO item = null;

        //  <PerformanceAddSchedulerAsync>

        Mono<CosmosItemResponse<CustomPOJO>> createItemPub = asyncContainer.createItem(item);
        createItemPub
                .publishOn(Schedulers.parallel())
                .subscribe(
                        itemResponse -> {
                            //this is now executed on reactor scheduler's parallel thread.
                            //reactor scheduler's parallel thread is meant for CPU intensive work.
                            veryCpuIntensiveWork();
                        });

        //  </PerformanceAddSchedulerAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/performance-tips-java-sdk-v4-sql
     * Performance tips - not specifying partition key in point-writes
     */

    /** Performance tips - not specifying partition key in point-writes */
    public static void PerformanceTipsJavaSDKv4NoPKSpecAsync() {

        CosmosAsyncContainer asyncContainer = null;
        CustomPOJO item = null;
        String pk = "pk_value";

        //  <PerformanceNoPKAsync>
        asyncContainer.createItem(item,new PartitionKey(pk),new CosmosItemRequestOptions()).block();
        //  </PerformanceNoPKAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/performance-tips-java-sdk-v4-sql
     * Performance tips - add partition key in point-writes
     */

    /** Performance tips - add partition key in point-writes */
    public static void PerformanceTipsJavaSDKv4AddPKSpecAsync() {

        CosmosAsyncContainer asyncContainer = null;
        CustomPOJO item = null;

        //  <PerformanceAddPKAsync>
        asyncContainer.createItem(item).block();
        //  </PerformanceAddPKAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/performance-tips-java-sdk-v4-sql
     * Performance tips - get request charge
     */

    /** Performance tips - get request charge */
    public static void PerformanceTipsJavaSDKv4RequestChargeSpecAsync() {

        CosmosAsyncContainer asyncContainer = null;
        CustomPOJO item = null;
        String pk = "pk_value";

        //  <PerformanceRequestChargeAsync>
        CosmosItemResponse<CustomPOJO> response = asyncContainer.createItem(item).block();

        response.getRequestCharge();
        //  </PerformanceRequestChargeAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/troubleshoot-java-sdk-v4-sql
     * Troubleshooting guide - needs scheduler
     * Async only
     */

    /** Troubleshooting guide - needs scheduler */
    public static void TroubleshootingGuideJavaSDKv4NeedsSchedulerAsync() {

        CosmosAsyncContainer container = null;
        CustomPOJO item = null;

        //  <TroubleshootNeedsSchedulerAsync>

        //Bad code with read timeout exception

        int requestTimeoutInSeconds = 10;

        /* ... */

        AtomicInteger failureCount = new AtomicInteger();
        // Max number of concurrent item inserts is # CPU cores + 1
        Flux<Family> familyPub =
                Flux.just(Families.getAndersenFamilyItem(), Families.getAndersenFamilyItem(), Families.getJohnsonFamilyItem());
        familyPub.flatMap(family -> {
            return container.createItem(family);
        }).flatMap(r -> {
            try {
                // Time-consuming work is, for example,
                // writing to a file, computationally heavy work, or just sleep.
                // Basically, it's anything that takes more than a few milliseconds.
                // Doing such operations on the IO Netty thread
                // without a proper scheduler will cause problems.
                // The subscriber will get a ReadTimeoutException failure.
                TimeUnit.SECONDS.sleep(2 * requestTimeoutInSeconds);
            } catch (Exception e) {
            }
            return Mono.empty();
        }).doOnError(Exception.class, exception -> {
            failureCount.incrementAndGet();
        }).blockLast();
        assert(failureCount.get() > 0);
        //  </TroubleshootNeedsSchedulerAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/troubleshoot-java-sdk-v4-sql
     * Troubleshooting guide - custom scheduler
     * Async only
     */

    /** Troubleshooting guide - custom scheduler */
    public static void TroubleshootingGuideJavaSDKv4CustomSchedulerAsync() {

        CosmosAsyncContainer container = null;
        CustomPOJO item = null;

        //  <TroubleshootCustomSchedulerAsync>
        // Have a singleton instance of an executor and a scheduler.
        ExecutorService ex  = Executors.newFixedThreadPool(30);
        Scheduler customScheduler = Schedulers.fromExecutor(ex);
        //  </TroubleshootCustomSchedulerAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/troubleshoot-java-sdk-v4-sql
     * Troubleshooting guide - publish on scheduler
     * Async only
     */

    /** Troubleshooting guide - publish on scheduler */
    public static void TroubleshootingGuideJavaSDKv4PublishOnSchedulerAsync() {

        CosmosAsyncContainer container = null;
        Scheduler customScheduler = null;
        Family family = null;

        //  <TroubleshootPublishOnSchedulerAsync>
        container.createItem(family)
                .publishOn(customScheduler) // Switches the thread.
                .subscribe(
                        // ...
                );
        //  </TroubleshootPublishOnSchedulerAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/tutorial-global-distribution-sql-api
     * Tutorial: Set up Azure Cosmos DB global distribution using the SQL API
     */

    /** Preferred locations */
    public static void TutorialSetUpAzureCosmosDBGlobalDistributionUsingTheSqlApiPreferredLocationsAsync() {
        String MASTER_KEY = "";
        String HOST = "";

        //  <TutorialGlobalDistributionPreferredLocationAsync>

        ArrayList<String> preferredRegions = new ArrayList<String>();
        preferredRegions.add("East US");
        preferredRegions.add( "West US");
        preferredRegions.add("Canada Central");

        CosmosAsyncClient client =
                new CosmosClientBuilder()
                        .endpoint(HOST)
                        .key(MASTER_KEY)
                        .preferredRegions(preferredRegions)
                        .contentResponseOnWriteEnabled(true)
                        .buildAsyncClient();

        //  </TutorialGlobalDistributionPreferredLocationAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/how-to-multi-master
     * Multi-master tutorial
     */

    /** Enable multi-master from client */
    public static void ConfigureMultimasterInYourApplicationsThatUseComosDBAsync() {
        String MASTER_KEY = "";
        String HOST = "";
        String region = "West US 2";

        //  <ConfigureMultimasterAsync>

        ArrayList<String> preferredRegions = new ArrayList<String>();
        preferredRegions.add(region);

        CosmosAsyncClient client =
                new CosmosClientBuilder()
                        .endpoint(HOST)
                        .key(MASTER_KEY)
                        .multipleWriteRegionsEnabled(true)
                        .preferredRegions(preferredRegions)
                        .buildAsyncClient();

        //  </ConfigureMultimasterAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/how-to-manage-consistency
     * Manage consistency
     */

    /** Managed consistency from client */
    public static void ManageConsistencyLevelsInAzureCosmosDBAsync() {
        String MASTER_KEY = "";
        String HOST = "";

        //  <ManageConsistencyAsync>

        CosmosAsyncClient client =
                new CosmosClientBuilder()
                        .endpoint(HOST)
                        .key(MASTER_KEY)
                        .consistencyLevel(ConsistencyLevel.EVENTUAL)
                        .buildAsyncClient();

        //  </ManageConsistencyAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/how-to-manage-consistency
     * Utilize session tokens
     */

    /** Session token */
    public static void ManageConsistencyLevelsInAzureCosmosDBSessionTokenAsync() {
        String itemId = "Henderson";
        String partitionKey = "4A3B-6Y78";

        CosmosAsyncContainer container = null;

        //  <ManageConsistencySessionAsync>

        // Get session token from response
        CosmosItemResponse<JsonNode> response = container.readItem(itemId, new PartitionKey(partitionKey), JsonNode.class).block();
        String sessionToken = response.getSessionToken();

        // Resume the session by setting the session token on the RequestOptions
        CosmosItemRequestOptions options = new CosmosItemRequestOptions();
        options.setSessionToken(sessionToken);
        CosmosItemResponse<JsonNode> response2 = container.readItem(itemId, new PartitionKey(partitionKey), JsonNode.class).block();

        //  </ManageConsistencySessionAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/how-to-manage-conflicts
     * Resolve conflicts, LWW policy
     */

    /** Client-side conflict resolution settings for LWW policy */
    public static void ManageConflictResolutionPoliciesInAzureCosmosDBLWWAsync() {
        String container_id = "family_container";
        String partition_key = "/pk";

        CosmosAsyncDatabase database = null;

        //  <ManageConflictResolutionLWWAsync>

        ConflictResolutionPolicy policy = ConflictResolutionPolicy.createLastWriterWinsPolicy("/myCustomId");

        CosmosContainerProperties containerProperties = new CosmosContainerProperties(container_id, partition_key);
        containerProperties.setConflictResolutionPolicy(policy);
        /* ...other container config... */
        database.createContainerIfNotExists(containerProperties).block();

        //  </ManageConflictResolutionLWWAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/how-to-manage-conflicts
     * Resolve conflicts, stored procedure
     */

    /** Client-side conflict resolution using stored procedure */
    public static void ManageConflictResolutionPoliciesInAzureCosmosDBSprocAsync() {
        String container_id = "family_container";
        String partition_key = "/pk";

        CosmosAsyncDatabase database = null;

        //  <ManageConflictResolutionSprocAsync>

        ConflictResolutionPolicy policy = ConflictResolutionPolicy.createCustomPolicy("resolver");

        CosmosContainerProperties containerProperties = new CosmosContainerProperties(container_id, partition_key);
        containerProperties.setConflictResolutionPolicy(policy);
        /* ...other container config... */
        database.createContainerIfNotExists(containerProperties).block();

        //  </ManageConflictResolutionSprocAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/how-to-manage-conflicts
     * Resolve conflicts, stored procedure
     */

    /** Client-side conflict resolution with fully custom policy */
    public static void ManageConflictResolutionPoliciesInAzureCosmosDBCustomAsync() {
        String container_id = "family_container";
        String partition_key = "/pk";

        CosmosAsyncDatabase database = null;

        //  <ManageConflictResolutionCustomAsync>

        ConflictResolutionPolicy policy = ConflictResolutionPolicy.createCustomPolicy();

        CosmosContainerProperties containerProperties = new CosmosContainerProperties(container_id, partition_key);
        containerProperties.setConflictResolutionPolicy(policy);
        /* ...other container config... */
        database.createContainerIfNotExists(containerProperties).block();

        //  </ManageConflictResolutionCustomAsync>
    }

    private static CosmosAsyncDatabase testDatabaseAsync = null;
    private static CosmosAsyncContainer testContainerAsync = null;

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/migrate-java-v4-sdk
     * Migrate previous versions to Java SDK v4
     */

    /** Migrate previous versions to Java SDK v4 */
    public static void MigrateJavaSDKv4ResourceAsync() {
        String container_id = "family_container";
        String partition_key = "/pk";

        CosmosAsyncDatabase database = null;

        // <MigrateJavaSDKv4ResourceAsync>

        // Create Async client.
        // Building an async client is still a sync operation.
        CosmosAsyncClient client = new CosmosClientBuilder()
                .endpoint("your.hostname")
                .key("yourmasterkey")
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .buildAsyncClient();

        // Create database with specified name
        client.createDatabaseIfNotExists("YourDatabaseName")
                .flatMap(databaseResponse -> {
                    testDatabaseAsync = client.getDatabase("YourDatabaseName");
                    // Container properties - name and partition key
                    CosmosContainerProperties containerProperties =
                            new CosmosContainerProperties("YourContainerName", "/id");

                    // Provision manual throughput
                    ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);

                    // Create container
                    return database.createContainerIfNotExists(containerProperties, throughputProperties);
                }).flatMap(containerResponse -> {
            testContainerAsync = database.getContainer("YourContainerName");
            return Mono.empty();
        }).subscribe();

        // </MigrateJavaSDKv4ResourceAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/migrate-java-v4-sdk
     * Item operations
     */

    /** Item operations */
    public static void MigrateJavaSDKv4ItemOperationsAsync() {
        String container_id = "family_container";
        String partition_key = "/pk";

        CosmosAsyncDatabase database = null;

        //  <MigrateItemOpsAsync>

        // Container is created. Generate many docs to insert.
        int number_of_docs = 50000;
        ArrayList<JsonNode> docs = generateManyDocs(number_of_docs);

        // Insert many docs into container...
        Flux.fromIterable(docs)
                .flatMap(doc -> testContainerAsync.createItem(doc))
                .subscribe(); // ...Subscribing triggers stream execution.

        //  </MigrateItemOpsAsync>
    }

    /** ^Helper function for the above code snippet */
    private static ArrayList<JsonNode> generateManyDocs(int number_of_docs) {
        //Dummy
        return null;
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/migrate-java-v4-sdk
     * Indexing
     */

    /** Indexing */
    public static void MigrateJavaSDKv4IndexingAsync() {
        String containerName = "family_container";
        String partition_key = "/pk";

        CosmosAsyncDatabase database = null;

        //  <MigrateIndexingAsync>

        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, "/lastName");

        // Custom indexing policy
        IndexingPolicy indexingPolicy = new IndexingPolicy();
        indexingPolicy.setIndexingMode(IndexingMode.CONSISTENT);

        // Included paths
        List<IncludedPath> includedPaths = new ArrayList<>();
        includedPaths.add(new IncludedPath("/*"));
        indexingPolicy.setIncludedPaths(includedPaths);

        // Excluded paths
        List<ExcludedPath> excludedPaths = new ArrayList<>();
        excludedPaths.add(new ExcludedPath("/name/*"));
        indexingPolicy.setExcludedPaths(excludedPaths);

        containerProperties.setIndexingPolicy(indexingPolicy);

        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);

        database.createContainerIfNotExists(containerProperties, throughputProperties);
        CosmosAsyncContainer containerIfNotExists = database.getContainer(containerName);

        //  </MigrateIndexingAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/migrate-java-v4-sdk
     * Stored procedure
     */

    /** Stored procedure */
    public static void MigrateJavaSDKv4SprocAsync() {
        String containerName = "family_container";
        String partition_key = "/pk";

        CosmosAsyncContainer container = null;

        //  <MigrateSprocAsync>

        logger.info("Creating stored procedure...\n");

        String sprocId = "createMyDocument";

        String sprocBody = "function createMyDocument() {\n" +
                "var documentToCreate = {\"id\":\"test_doc\"}\n" +
                "var context = getContext();\n" +
                "var collection = context.getCollection();\n" +
                "var accepted = collection.createDocument(collection.getSelfLink(), documentToCreate,\n" +
                "    function (err, documentCreated) {\n" +
                "if (err) throw new Error('Error' + err.message);\n" +
                "context.getResponse().setBody(documentCreated.id)\n" +
                "});\n" +
                "if (!accepted) return;\n" +
                "}";

        CosmosStoredProcedureProperties storedProcedureDef = new CosmosStoredProcedureProperties(sprocId, sprocBody);
        container.getScripts()
                .createStoredProcedure(storedProcedureDef,
                        new CosmosStoredProcedureRequestOptions()).block();

        // ...

        logger.info(String.format("Executing stored procedure %s...\n\n", sprocId));

        CosmosStoredProcedureRequestOptions options = new CosmosStoredProcedureRequestOptions();
        options.setPartitionKey(new PartitionKey("test_doc"));

        container.getScripts()
                .getStoredProcedure(sprocId)
                .execute(null, options)
                .flatMap(executeResponse -> {
                    logger.info(String.format("Stored procedure %s returned %s (HTTP %d), at cost %.3f RU.\n",
                            sprocId,
                            executeResponse.getResponseAsString(),
                            executeResponse.getStatusCode(),
                            executeResponse.getRequestCharge()));
                    return Mono.empty();
                }).block();

        //  </MigrateSprocAsync>
    }

    private static ObjectMapper OBJECT_MAPPER = null;

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/migrate-java-v4-sdk
     * Change Feed
     */

    /** Change Feed */
    public static void MigrateJavaSDKv4CFAsync() {
        String hostName = "hostname";
        String partition_key = "/pk";

        CosmosAsyncContainer feedContainer = null;
        CosmosAsyncContainer leaseContainer = null;

        //  <MigrateCFAsync>

        ChangeFeedProcessor changeFeedProcessorInstance =
                new ChangeFeedProcessorBuilder()
                        .hostName(hostName)
                        .feedContainer(feedContainer)
                        .leaseContainer(leaseContainer)
                        .handleChanges((List<JsonNode> docs) -> {
                            logger.info("--->setHandleChanges() START");

                            for (JsonNode document : docs) {
                                try {
                                    //Change Feed hands the document to you in the form of a JsonNode
                                    //As a developer you have two options for handling the JsonNode document provided to you by Change Feed
                                    //One option is to operate on the document in the form of a JsonNode, as shown below. This is great
                                    //especially if you do not have a single uniform data model for all documents.
                                    logger.info("---->DOCUMENT RECEIVED: " + OBJECT_MAPPER.writerWithDefaultPrettyPrinter()
                                            .writeValueAsString(document));

                                    //You can also transform the JsonNode to a POJO having the same structure as the JsonNode,
                                    //as shown below. Then you can operate on the POJO.
                                    CustomPOJO pojo_doc = OBJECT_MAPPER.treeToValue(document, CustomPOJO.class);
                                    logger.info("----=>id: " + pojo_doc.getId());

                                } catch (JsonProcessingException e) {
                                    e.printStackTrace();
                                }
                            }
                            logger.info("--->handleChanges() END");

                        })
                        .buildChangeFeedProcessor();

        // ...

        changeFeedProcessorInstance.start()
                .subscribeOn(Schedulers.elastic())
                .subscribe();

        //  </MigrateCFAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/migrate-java-v4-sdk
     * Container TTL
     */

    /** Container TTL */
    public static void MigrateJavaSDKv4ContainerTTLAsync() {
        String hostName = "hostname";
        String partition_key = "/pk";

        CosmosAsyncDatabase database = null;

        //  <MigrateContainerTTLAsync>

        CosmosAsyncContainer container;

        // Create a new container with TTL enabled with default expiration value
        CosmosContainerProperties containerProperties = new CosmosContainerProperties("myContainer", "/myPartitionKey");
        containerProperties.setDefaultTimeToLiveInSeconds(90 * 60 * 60 * 24);
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);
        database.createContainerIfNotExists(containerProperties, throughputProperties).block();
        container = database.getContainer("myContainer");

        //  </MigrateContainerTTLAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/migrate-java-v4-sdk
     * Item TTL
     */

    /** Item TTL */
    public static void MigrateJavaSDKv4ItemTTLAsync() {
        String hostName = "hostname";
        String partition_key = "/pk";

        CosmosAsyncDatabase database = null;

        //  <MigrateItemTTLAsync>

        // Set the value to the expiration in seconds
        SalesOrder salesOrder = new SalesOrder(
                "SO05",
                "CO18009186470",
                60 * 60 * 24 * 30  // Expire sales orders in 30 days
        );

        //  </MigrateItemTTLAsync>
    }

}

//  <MigrateItemTTLClassAsync>

// Include a property that serializes to "ttl" in JSON
class SalesOrder
{
    private String id;
    private String customerId;
    private Integer ttl;

    public SalesOrder(String id, String customerId, Integer ttl) {
        this.id = id;
        this.customerId = customerId;
        this.ttl = ttl;
    }

    public String getId() {return this.id;}
    public void setId(String new_id) {this.id = new_id;}
    public String getCustomerId() {return this.customerId;}
    public void setCustomerId(String new_cid) {this.customerId = new_cid;}
    public Integer getTtl() {return this.ttl;}
    public void setTtl(Integer new_ttl) {this.ttl = new_ttl;}

    //...
}

//  </MigrateItemTTLClassAsync>
