// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.nonpartitioncontainercrud;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class NonPartitionContainerCrudQuickStart {

    private static final Logger logger = LoggerFactory.getLogger(NonPartitionContainerCrudQuickStart.class);

    public static void main(String[] args) {
        CosmosClient cosmosClient = new CosmosClientBuilder()
            .endpoint(AccountSettings.HOST)
            .key(AccountSettings.MASTER_KEY)
            .contentResponseOnWriteEnabled(true)
            .buildClient();

        //  Get the database and container which are pre-created using v2 versions of the SDK
        //  such that they don't contain partition key definition.
        CosmosDatabase cosmosDatabase = cosmosClient.getDatabase("testdb");
        CosmosContainer cosmosContainer = cosmosDatabase.getContainer("testcontainer");


        Family family = new Family("id-1", "John", "Doe", "Doe");
        logger.info("Creating Item");
        cosmosContainer.createItem(family, new PartitionKey(family._partitionKey), new CosmosItemRequestOptions());
        logger.info("Item created");

        logger.info("Creating items through bulk");
        family = new Family("id-2", "Jane", "Doe", "Doe");
        CosmosItemOperation createItemOperation = CosmosBulkOperations.getCreateItemOperation(family,
            new PartitionKey(family._partitionKey));
        cosmosContainer.executeBulkOperations(Collections.singletonList(createItemOperation));
        logger.info("Items through bulk created");

        //  To read an existing document which doesn't have any partition key associated with it,
        //  read it using PartitionKey.NONE
        CosmosItemResponse<JsonNode> cosmosItemResponse = cosmosContainer.readItem("itemId", PartitionKey.NONE, JsonNode.class);
        logger.info("Cosmos Item response is : {}", cosmosItemResponse.getItem().toPrettyString());
    }

    static class Family {
        public String id;
        public String firstName;
        public String lastName;
        public String _partitionKey;

        public Family(String id, String firstName, String lastName, String _partitionKey) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this._partitionKey = _partitionKey;
        }
    }
}
