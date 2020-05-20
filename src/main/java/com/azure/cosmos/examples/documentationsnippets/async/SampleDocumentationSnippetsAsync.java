// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.documentationsnippets.async;

import com.azure.cosmos.ConnectionPolicy;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosClientException;
import com.azure.cosmos.CosmosPagedFlux;
import com.azure.cosmos.examples.changefeed.SampleChangeFeedProcessor;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.CustomPOJO;
import com.azure.cosmos.examples.storedprocedure.async.SampleStoredProcedureAsync;
import com.azure.cosmos.models.CosmosAsyncItemResponse;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosStoredProcedureProperties;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.FeedOptions;
import com.azure.cosmos.models.PartitionKey;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.concurrent.CountDownLatch;

public class SampleDocumentationSnippetsAsync {

    private CosmosAsyncClient client;

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    protected static Logger logger = LoggerFactory.getLogger(SampleChangeFeedProcessor.class.getSimpleName());

    /**
     * This file organizes Azure Docs Azure Cosmos DB code snippets to enable easily upgrading to new Maven artifacts.
     * Usage: upgrade pom.xml to the latest Java SDK Maven artifact; rebuild this project; correct any public surface changes.
     * <p>
     * -
     */
    //  <Main>
    public static void main(String[] args) {
        // Do nothing. This file is meant to be built but not executed.
    }

}
