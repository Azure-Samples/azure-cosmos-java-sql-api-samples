// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.examples.customitemserializer.async;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.CosmosItemSerializer;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Families;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.MapType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CustomItemSerializerQuickStart {
    private CosmosAsyncClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    private final static Logger logger = LoggerFactory.getLogger(
        com.azure.cosmos.examples.customitemserializer.async.CustomItemSerializerQuickStart.class);

    public void close() {
        client.close();
    }

    /**
     * Run a Hello CosmosDB console application.
     * <p>
     * This is a simple sample application intended to demonstrate usage of custom serializers for items/documents
     * This sample will
     * 1. Create asynchronous client, database and container instances
     * 2. Create and read (wrapped and unwrapped)
     * 3. Delete the Cosmos DB database and container resources and close the client.
     */
    //  <Main>
    public static void main(String[] args) {
        com.azure.cosmos.examples.customitemserializer.async.CustomItemSerializerQuickStart p =
            new com.azure.cosmos.examples.customitemserializer.async.CustomItemSerializerQuickStart();

        try {
            logger.info("Starting ASYNC main");
            p.getStartedDemo();
            logger.info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(String.format("Cosmos getStarted failed with %s", e));
        } finally {
            logger.info("Closing the client");
            p.shutdown();
        }
    }

    //  </Main>

    private void getStartedDemo() {

        logger.info("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        ArrayList<String> preferredRegions = new ArrayList<>();
        preferredRegions.add("West US");

        //  Setting the preferred location to Cosmos DB Account region
        //  West US is just an example. User should set preferred location to the Cosmos DB region closest to the application

        //  Create async client
        //  <CreateAsyncClient>
        client = new CosmosClientBuilder()
            .endpoint(AccountSettings.HOST)
            .key(AccountSettings.MASTER_KEY)
            .preferredRegions(preferredRegions)
            .contentResponseOnWriteEnabled(true)
            .consistencyLevel(ConsistencyLevel.SESSION)
            // This modifies the default serializer for document/item operations
            // the serializer can be overridden for every operation separately via request options when needed
            .customItemSerializer(MySampleItemSerializer.INSTANCE)
            .buildAsyncClient();

        //  </CreateAsyncClient>

        createDatabaseIfNotExists();
        createContainerIfNotExists();

        Family andersenFamilyItem = Families.getAndersenFamilyItem();

        logger.info("Creates an item in the container (wrapped in an envelope).");
        createFamily(andersenFamilyItem);

        logger.info("Reads the item with custom serializer (unwrapping the envelope).");
        Family readFamilyUnwrapped = readFamily(andersenFamilyItem, true, Family.class);
        ObjectNode readFamilyAsTreeUnwrapped = readFamily(andersenFamilyItem, true, ObjectNode.class);
        logger.info("Unwrapped json: {}", readFamilyAsTreeUnwrapped);
        ObjectNode readFamilyStillWrapped = readFamily(andersenFamilyItem, false, ObjectNode.class);
        logger.info("Wrapped envelope json: {}", readFamilyStillWrapped);
    }

    private void createDatabaseIfNotExists() {
        logger.info("Create database " + databaseName + " if not exists.");

        //  Create database if not exists
        //  <CreateDatabaseIfNotExists>
        Mono<CosmosDatabaseResponse> databaseIfNotExists = client.createDatabaseIfNotExists(databaseName);
        databaseIfNotExists.flatMap(databaseResponse -> {
            database = client.getDatabase(databaseResponse.getProperties().getId());
            logger.info("Checking database " + database.getId() + " completed!\n");
            return Mono.empty();
        }).block();
        //  </CreateDatabaseIfNotExists>
    }

    private void createContainerIfNotExists() {
        logger.info("Create container " + containerName + " if not exists.");

        //  Create container if not exists
        //  <CreateContainerIfNotExists>
        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, "/id");
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);
        Mono<CosmosContainerResponse> containerIfNotExists = database.createContainerIfNotExists(containerProperties, throughputProperties);

        //  Create container with 400 RU/s
        CosmosContainerResponse cosmosContainerResponse = containerIfNotExists.block();
        container = database.getContainer(cosmosContainerResponse.getProperties().getId());
        //  </CreateContainerIfNotExists>
    }

    private void createFamily(Family family) {
        //  <CreateItem>
        try {

            //  Combine multiple item inserts, associated success println's, and a final aggregate stats println into one Reactive stream.
            double charge = container
                .createItem(family)
                .map(itemResponse -> {
                    logger.info(String.format("Created item with request charge of %.2f within" +
                            " duration %s",
                        itemResponse.getRequestCharge(), itemResponse.getDuration()));
                    logger.info(String.format("Item ID: %s\n", itemResponse.getItem().getId()));
                    return itemResponse.getRequestCharge();
                })
                .block(); //Preserve the total charge and print aggregate charge/item count stats.

            logger.info(String.format("Created items with total request charge of %.2f\n", charge));

        } catch (Exception err) {
            if (err instanceof CosmosException) {
                //Client-specific errors
                CosmosException cerr = (CosmosException) err;
                cerr.printStackTrace();
                logger.error(String.format("Read Item failed with %s\n", cerr));
            } else {
                //General errors
                err.printStackTrace();
            }
        }

        //  </CreateItem>
    }

    private <T> T readFamily(Family family, boolean useCustomSerializer, Class<T> clazz) {
        //  Using partition key for point read scenarios.
        //  This will help fast look up of items because of partition key
        //  <ReadItem>

        CosmosItemRequestOptions requestOptions = new CosmosItemRequestOptions();

        // we configured the custom serializer on the CosmosClientBuilder - so, it will be used by default
        // if for certain operations the default serializer should be used instead the request options
        // can override the serializer to be used
        if (!useCustomSerializer) {
            requestOptions.setCustomItemSerializer(CosmosItemSerializer.DEFAULT_SERIALIZER);
        }

        try {
                return container
                    .readItem(family.getId(), new PartitionKey(family.getId()), requestOptions, clazz)
                    .map(itemResponse -> {
                        double requestCharge = itemResponse.getRequestCharge();
                        Duration requestLatency = itemResponse.getDuration();
                        logger.info(String.format("Item successfully read with id %s with a charge of %.2f and within duration %s",
                            family.getId(), requestCharge, requestLatency));

                        return itemResponse.getItem();
                    })
                    .block();

        } catch (Exception err) {
            if (err instanceof CosmosException) {
                //Client-specific errors
                CosmosException cerr = (CosmosException) err;
                cerr.printStackTrace();
                logger.error(String.format("Read Item failed with %s\n", cerr));
            } else {
                //General errors
                err.printStackTrace();
            }

            throw err;
        }
        //  </ReadItem>
    }

    private void shutdown() {
        try {
            //Clean shutdown
            logger.info("Deleting Cosmos DB resources");
            logger.info("-Deleting container...");
            if (container != null)
                container.delete().subscribe();
            logger.info("-Deleting database...");
            if (database != null)
                database.delete().subscribe();
            logger.info("-Closing the client...");
        } catch (Exception err) {
            logger.error("Deleting Cosmos DB resources failed, will still attempt to close the client. See stack trace below.");
            err.printStackTrace();
        }
        client.close();
        logger.info("Done.");
    }

    // Sample custom serializer - for demonstration purposes it will wrap the actual document in a
    // "wrappedContent" node - so, each document will be an envelope - with the actual payload in
    // the "wrappedContent" property. This is typically done when the actual payload is compressed or
    // using some non-json data format etc.
    private static class MySampleItemSerializer extends CosmosItemSerializer {
        public static CosmosItemSerializer INSTANCE = new MySampleItemSerializer();
        private final static ObjectMapper customObjectMapper = new ObjectMapper();

        private final static MapType JACKSON_MAP_TYPE = customObjectMapper
            .getTypeFactory()
            .constructMapType(
                LinkedHashMap.class,
                String.class,
                Object.class);

        private MySampleItemSerializer() {}

        @Override
        public <T> Map<String, Object> serialize(T item) {
            Map<String, Object> payload = customObjectMapper.convertValue(item, JACKSON_MAP_TYPE);

            Map<String, Object>  wrappedJsonTree = new ConcurrentHashMap<>();
            wrappedJsonTree.put("id", payload.get("id"));
            wrappedJsonTree.put("wrappedContent", payload);

            return wrappedJsonTree;
        }

        @Override
        public <T> T deserialize(Map<String, Object> jsonNodeMap, Class<T> clazz) {
            ObjectNode tree = customObjectMapper.convertValue(jsonNodeMap, ObjectNode.class);
            return customObjectMapper.convertValue(tree.get("wrappedContent"), clazz);
        }
    }
}
