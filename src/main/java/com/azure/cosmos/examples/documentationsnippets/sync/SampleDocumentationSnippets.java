// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.documentationsnippets.sync;

import com.azure.cosmos.ConnectionPolicy;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosPagedIterable;
import com.azure.cosmos.examples.changefeed.SampleChangeFeedProcessor;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.CustomPOJO;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosStoredProcedureProperties;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureResponse;
import com.azure.cosmos.models.FeedOptions;
import com.azure.cosmos.models.PartitionKey;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;

public class SampleDocumentationSnippets {

    private CosmosClient client;

    private CosmosDatabase database;
    private CosmosContainer container;

    protected static Logger logger = LoggerFactory.getLogger(SampleChangeFeedProcessor.class.getSimpleName());

    /**
     * This file organizes Azure Docs Azure Cosmos DB sync code snippets to enable easily upgrading to new Maven artifacts.
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
    public static void TutorialSetUpAzureCosmosDBGlobalDistributionUsingTheSqlApiPreferredLocationsSync() {
        String MASTER_KEY = "";
        String HOST = "";

        //  <TutorialGlobalDistributionPreferredLocationSync>

        ConnectionPolicy policy = new ConnectionPolicy();
        policy.setUsingMultipleWriteLocations(true);
        policy.setPreferredLocations(Arrays.asList("East US", "West US", "Canada Central"));
        CosmosClient client =
                new CosmosClientBuilder()
                        .endpoint(HOST)
                        .key(MASTER_KEY)
                        .connectionPolicy(policy)
                        .buildClient();

        //  </TutorialGlobalDistributionPreferredLocationSync>
    }

}
