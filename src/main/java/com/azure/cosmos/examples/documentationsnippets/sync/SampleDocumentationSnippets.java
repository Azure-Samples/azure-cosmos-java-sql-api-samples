// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.documentationsnippets.sync;

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
import com.azure.cosmos.models.ConflictResolutionPolicy;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosStoredProcedureProperties;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.PartitionKeyDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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

        ArrayList<String> preferredRegions = new ArrayList<String>();
        preferredRegions.add("East US");
        preferredRegions.add( "West US");
        preferredRegions.add("Canada Central");

        CosmosClient client =
                new CosmosClientBuilder()
                        .endpoint(HOST)
                        .key(MASTER_KEY)
                        .preferredRegions(preferredRegions)
                        .buildClient();

        //  </TutorialGlobalDistributionPreferredLocationSync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/how-to-multi-master
     * Multi-master tutorial
     */

    /** Enable multi-master from client */
    public static void ConfigureMultimasterInYourApplicationsThatUseComosDBSync() {
        String MASTER_KEY = "";
        String HOST = "";
        String region = "West US 2";

        //  <ConfigureMultimasterSync>

        ArrayList<String> preferredRegions = new ArrayList<String>();
        preferredRegions.add(region);

        CosmosClient client =
                new CosmosClientBuilder()
                        .endpoint(HOST)
                        .key(MASTER_KEY)
                        .multipleWriteRegionsEnabled(true)
                        .preferredRegions(preferredRegions)
                        .buildClient();

        //  </ConfigureMultimasterSync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/how-to-manage-consistency
     * Manage consistency
     */

    /** Managed consistency from client */
    public static void ManageConsistencyLevelsInAzureCosmosDBSync() {
        String MASTER_KEY = "";
        String HOST = "";

        //  <ManageConsistencySync>

        CosmosClient client =
                new CosmosClientBuilder()
                        .endpoint(HOST)
                        .key(MASTER_KEY)
                        .consistencyLevel(ConsistencyLevel.EVENTUAL)
                        .buildClient();

        //  </ManageConsistencySync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/how-to-manage-conflicts
     * Resolve conflicts, LWW policy
     */

    /** Client-side conflict resolution settings for LWW policy */
    public static void ManageConflictResolutionPoliciesInAzureCosmosDBLWWSync() {
        String container_id = "family_container";
        String partition_key = "/pk";

        CosmosDatabase database = null;

        //  <ManageConflictResolutionLWWSync>

        ConflictResolutionPolicy policy = ConflictResolutionPolicy.createLastWriterWinsPolicy("/myCustomId");

        CosmosContainerProperties containerProperties = new CosmosContainerProperties(container_id, partition_key);
        containerProperties.setConflictResolutionPolicy(policy);
        /* ...other container config... */
        database.createContainerIfNotExists(containerProperties);

        //  </ManageConflictResolutionLWWSync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/how-to-manage-conflicts
     * Resolve conflicts, stored procedure
     */

    /** Client-side conflict resolution using stored procedure */
    public static void ManageConflictResolutionPoliciesInAzureCosmosDBSprocSync() {
        String container_id = "family_container";
        String partition_key = "/pk";

        CosmosDatabase database = null;

        //  <ManageConflictResolutionSprocSync>

        ConflictResolutionPolicy policy = ConflictResolutionPolicy.createCustomPolicy("resolver");

        CosmosContainerProperties containerProperties = new CosmosContainerProperties(container_id, partition_key);
        containerProperties.setConflictResolutionPolicy(policy);
        /* ...other container config... */
        database.createContainerIfNotExists(containerProperties);

        //  </ManageConflictResolutionSprocSync>
    }

    /**
     * https://docs.microsoft.com/en-us/azure/cosmos-db/how-to-manage-conflicts
     * Resolve conflicts, stored procedure
     */

    /** Client-side conflict resolution with fully custom policy */
    public static void ManageConflictResolutionPoliciesInAzureCosmosDBCustomSync() {
        String container_id = "family_container";
        String partition_key = "/pk";

        CosmosDatabase database = null;

        //  <ManageConflictResolutionCustomSync>

        ConflictResolutionPolicy policy = ConflictResolutionPolicy.createCustomPolicy();

        CosmosContainerProperties containerProperties = new CosmosContainerProperties(container_id, partition_key);
        containerProperties.setConflictResolutionPolicy(policy);
        /* ...other container config... */
        database.createContainerIfNotExists(containerProperties);

        //  </ManageConflictResolutionCustomSync>
    }

}
