// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.documentationsnippets.async;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.examples.changefeed.SampleChangeFeedProcessor;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.CustomPOJO;
import com.azure.cosmos.examples.storedprocedure.async.SampleStoredProcedureAsync;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosStoredProcedureProperties;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

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

}
