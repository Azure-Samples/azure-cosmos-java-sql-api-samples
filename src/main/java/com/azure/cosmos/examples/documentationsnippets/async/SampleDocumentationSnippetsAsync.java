// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.documentationsnippets.async;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.examples.changefeed.SampleChangeFeedProcessor;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.CustomPOJO;
import com.azure.cosmos.examples.common.Families;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.examples.storedprocedure.async.SampleStoredProcedureAsync;
import com.azure.cosmos.models.ConflictResolutionPolicy;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosStoredProcedureProperties;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SampleDocumentationSnippetsAsync {

    private CosmosAsyncClient client;

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    protected static Logger logger = LoggerFactory.getLogger(SampleChangeFeedProcessor.class.getSimpleName());

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
    public static void PerformanceTipsJavaSDKv4AddSchedulerSync() {

        CosmosAsyncContainer asyncContainer = null;
        CustomPOJO item = null;

        //  <PerformanceAddSchedulerAsync>

        Mono<CosmosItemResponse<CustomPOJO>> createItemPub = asyncContainer.createItem(item);
        createItemPub
                .subscribeOn(Schedulers.elastic())
                .subscribe(
                        itemResponse -> {
                            //this is executed on eventloop IO netty thread.
                            //the eventloop thread is shared and is meant to return back quickly.
                            //
                            // DON'T do this on eventloop IO netty thread.
                            veryCpuIntensiveWork();
                        });

        //  </PerformanceAddSchedulerAsync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/troubleshoot-java-sdk-v4-sql
     * Troubleshooting guide - needs scheduler
     * Async only
     */

    /** Troubleshooting guidie - needs scheduler */
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

}
