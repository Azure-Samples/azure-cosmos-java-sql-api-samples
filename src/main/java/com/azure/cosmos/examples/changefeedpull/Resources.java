// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.changefeedpull;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

public class Resources {
    CosmosAsyncContainer container;
    CosmosAsyncDatabase database;
    CosmosAsyncClient clientAsync;
    public String DATABASE_NAME;
    public String COLLECTION_NAME;
    public final ArrayListValuedHashMap<String, ObjectNode> partitionKeyToDocuments = new ArrayListValuedHashMap<String, ObjectNode>();
    protected static Logger logger = LoggerFactory.getLogger(Resources.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static String PARTITION_KEY_FIELD_NAME;

    public Resources(String PARTITION_KEY_FIELD_NAME, CosmosAsyncClient clientAsync,
            String DATABASE_NAME, String COLLECTION_NAME) {
        this.clientAsync = clientAsync;
        this.COLLECTION_NAME = COLLECTION_NAME;
        this.DATABASE_NAME = DATABASE_NAME;
        Resources.PARTITION_KEY_FIELD_NAME = PARTITION_KEY_FIELD_NAME;
        this.container = this.createContainer();
    }

    void insertDocuments(
            int partitionCount,
            int documentCount) {

        List<ObjectNode> docs = new ArrayList<>();

        for (int i = 0; i < partitionCount; i++) {
            String partitionKey = UUID.randomUUID().toString();
            for (int j = 0; j < documentCount; j++) {
                docs.add(getDocumentDefinition(partitionKey));
            }
        }

        ArrayList<Mono<CosmosItemResponse<ObjectNode>>> result = new ArrayList<>();
        for (int i = 0; i < docs.size(); i++) {
            result.add(container
                    .createItem(docs.get(i)));
        }

        List<ObjectNode> insertedDocs = Flux.merge(
                Flux.fromIterable(result),
                10)
                .map(CosmosItemResponse::getItem).collectList().block();

        for (ObjectNode doc : insertedDocs) {
            partitionKeyToDocuments.put(
                    doc.get(PARTITION_KEY_FIELD_NAME).textValue(),
                    doc);
        }
        logger.info("FINISHED INSERT");
    }

    void deleteDocuments(
            int partitionCount,
            int documentCount) {

        Collection<ObjectNode> docs;
        for (int i = 0; i < partitionCount; i++) {
            String partitionKey = this.partitionKeyToDocuments
                    .keySet()
                    .stream()
                    .skip(i)
                    .findFirst()
                    .get();

            docs = this.partitionKeyToDocuments.get(partitionKey);

            for (int j = 0; j < documentCount; j++) {
                ObjectNode docToBeDeleted = docs.stream().findFirst().get();
                container.deleteItem(docToBeDeleted, null).block();
                docs.remove(docToBeDeleted);
            }
        }
        logger.info("FINISHED DELETE");
    }

    void updateDocuments(
            int partitionCount,
            int documentCount) {

        Collection<ObjectNode> docs;
        for (int i = 0; i < partitionCount; i++) {
            String partitionKey = this.partitionKeyToDocuments
                    .keySet()
                    .stream()
                    .skip(i)
                    .findFirst()
                    .get();

            docs = this.partitionKeyToDocuments.get(partitionKey);

            for (int j = 0; j < documentCount; j++) {
                ObjectNode docToBeUpdated = docs.stream().skip(j).findFirst().get();
                docToBeUpdated.put("someProperty", UUID.randomUUID().toString());
                container.replaceItem(
                        docToBeUpdated,
                        docToBeUpdated.get("id").textValue(),
                        new PartitionKey(docToBeUpdated.get(PARTITION_KEY_FIELD_NAME).textValue()),
                        null).block();
            }
        }
        logger.info("FINISHED UPSERT");
    }

    private static ObjectNode getDocumentDefinition(String partitionKey) {
        String uuid = UUID.randomUUID().toString();
        String json = String.format("{ "
                + "\"id\": \"%s\", "
                + "\"" + PARTITION_KEY_FIELD_NAME + "\": \"%s\", "
                + "\"prop\": \"%s\""
                + "}", uuid, partitionKey, uuid);

        try {
            return OBJECT_MAPPER.readValue(json, ObjectNode.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Invalid partition key value provided.");
        }
    }

    public static CosmosAsyncDatabase createNewDatabase(CosmosAsyncClient client, String databaseName) {
        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName).block();
        return client.getDatabase(databaseResponse.getProperties().getId());
    }

    public static void deleteDatabase(CosmosAsyncDatabase createdDatabase2) {
        createdDatabase2.delete().block();
    }

    public void createNewCollection(CosmosAsyncClient clientAsync2, String databaseName,
            String collectionName) {
        Mono<CosmosDatabaseResponse> databaseResponse = clientAsync2.createDatabaseIfNotExists(databaseName);
        CosmosAsyncDatabase database = clientAsync2.getDatabase(databaseResponse.block().getProperties().getId());
        CosmosContainerProperties containerSettings = new CosmosContainerProperties(collectionName,
                "/" + PARTITION_KEY_FIELD_NAME);
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);
        Mono<CosmosContainerResponse> containerResponse = database.createContainerIfNotExists(containerSettings,
                throughputProperties);
        this.database = clientAsync.getDatabase(database.getId());
        this.container = clientAsync.getDatabase(DATABASE_NAME).getContainer(COLLECTION_NAME);
        containerResponse.block();
    }

    private CosmosAsyncContainer createContainer() {
        createNewCollection(clientAsync, DATABASE_NAME,
                COLLECTION_NAME);
        
        return database.getContainer(COLLECTION_NAME);
    }

}
