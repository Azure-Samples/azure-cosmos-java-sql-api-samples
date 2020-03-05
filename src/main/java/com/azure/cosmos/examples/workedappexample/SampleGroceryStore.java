// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.examples.workedappexample;

import com.azure.cosmos.ChangeFeedProcessor;
import com.azure.cosmos.ConnectionPolicy;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncContainerResponse;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosClientException;
import com.azure.cosmos.CosmosContainerProperties;
import com.azure.cosmos.CosmosContainerRequestOptions;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.implementation.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Sample for Change Feed Processor.
 *
 */
public class SampleGroceryStore {

    public static int WAIT_FOR_WORK = 60000;
    public static final String DATABASE_NAME = "db_" + RandomStringUtils.randomAlphabetic(7);
    public static final String COLLECTION_NAME = "coll_" + RandomStringUtils.randomAlphabetic(7);
    private static final ObjectMapper OBJECT_MAPPER = Utils.getSimpleObjectMapper();
    protected static Logger logger = LoggerFactory.getLogger(SampleGroceryStore.class.getSimpleName());


    private static ChangeFeedProcessor changeFeedProcessorInstance;
    private static boolean isWorkCompleted = false;

    private static CosmosAsyncContainer typeContainer;
    private static CosmosAsyncContainer expiryDateContainer;

    public static void main (String[]args) {
        logger.info("BEGIN Sample");

        try {

            System.out.println("-->CREATE DocumentClient");
            CosmosAsyncClient client = getCosmosClient();

            System.out.println("-->CREATE Contoso Grocery Store database: " + DATABASE_NAME);
            CosmosAsyncDatabase cosmosDatabase = createNewDatabase(client, DATABASE_NAME);

            System.out.println("-->CREATE container for store inventory: " + COLLECTION_NAME);
            CosmosAsyncContainer feedContainer = createNewCollection(client, DATABASE_NAME, COLLECTION_NAME, "/id");

            System.out.println("-->CREATE container for lease: " + COLLECTION_NAME + "-leases");
            CosmosAsyncContainer leaseContainer = createNewLeaseCollection(client, DATABASE_NAME, COLLECTION_NAME + "-leases");

            System.out.println("-->CREATE container for materialized view partitioned by 'type': " + COLLECTION_NAME + "-leases");
            typeContainer = createNewCollection(client, DATABASE_NAME, COLLECTION_NAME + "-pktype", "/type");

            System.out.println("-->CREATE container for materialized view with aggregation rule based on days until expiration " + COLLECTION_NAME + "-leases");
            expiryDateContainer = createNewCollection(client, DATABASE_NAME, COLLECTION_NAME + "-pkexpiryDate", "/expiryDaysRemaining");

            changeFeedProcessorInstance = getChangeFeedProcessor("SampleHost_1", feedContainer, leaseContainer);
            changeFeedProcessorInstance.start()
                .subscribeOn(Schedulers.elastic())
                .doOnSuccess(aVoid -> {
                    //Insert 10 documents into the feed container
                    //createNewDocumentsJSON demonstrates how to insert a JSON object into a Cosmos DB container as an item
                    createNewDocumentsJSON(feedContainer, 10, Duration.ofSeconds(3));
                    isWorkCompleted = true;
                })
                .subscribe();

            long remainingWork = WAIT_FOR_WORK;
            while (!isWorkCompleted && remainingWork > 0) {
                Thread.sleep(100);
                remainingWork -= 100;
            }

            if (isWorkCompleted) {
                if (changeFeedProcessorInstance != null) {
                    changeFeedProcessorInstance.stop().subscribe();
                }
            } else {
                throw new RuntimeException("The change feed processor initialization and automatic create document feeding process did not complete in the expected time");
            }

            System.out.println("-->DELETE sample's database: " + DATABASE_NAME);
            deleteDatabase(cosmosDatabase);

            Thread.sleep(500);

        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("END Sample");
    }

    public static ChangeFeedProcessor getChangeFeedProcessor(String hostName, CosmosAsyncContainer feedContainer, CosmosAsyncContainer leaseContainer) {
        return ChangeFeedProcessor.changeFeedProcessorBuilder()
            .setHostName(hostName)
            .setFeedContainer(feedContainer)
            .setLeaseContainer(leaseContainer)
            .setHandleChanges((List<JsonNode> docs) -> {
                //System.out.println("--->setHandleChanges() START");

                for (JsonNode document : docs) {
                     /*   System.out.println("---->DOCUMENT RECEIVED: " + OBJECT_MAPPER.writerWithDefaultPrettyPrinter()
                                                                            .writeValueAsString(document)); */
                        //Each document update from the feed container branches out to all three user services
                        updateInventoryAlertService(document);
                        updateInventoryTypeMaterializedView(document);
                        updateInventoryExpiryDateAggregationPolicyMaterializedView(document);

                        //Forward document =>
                }
                //System.out.println("--->handleChanges() END");

            })
            .build();
    }

    private static void updateInventoryAlertService(JsonNode document) {
        System.out.println("Alert: Added new item of type " + document.get("type") + "\n");
    }

    private static void updateInventoryTypeMaterializedView(JsonNode document) {
        typeContainer.createItem(document).subscribe();
    }

    private static void updateInventoryExpiryDateAggregationPolicyMaterializedView(JsonNode document) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode transformed_document = null;

        //Deep-copy the input document
        try {
            transformed_document = document.deepCopy();
        } catch (Exception e) {
            e.printStackTrace();
        }

        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

        try {
            long days_passed = TimeUnit.MILLISECONDS.toDays
                    (
                            ((Date) formatter.parse("2020-03-30")).getTime() - ((Date) formatter.parse(document.get("expiryDate").textValue())).getTime()
                    );

            ((ObjectNode)transformed_document).remove("expiryDate");
            ((ObjectNode) transformed_document).put("expiryDaysRemaining", String.format("%d", days_passed));

            expiryDateContainer.createItem(transformed_document).subscribe();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        typeContainer.createItem(document).subscribe();
    }

    public static CosmosAsyncClient getCosmosClient() {

        return new CosmosClientBuilder()
                .setEndpoint(AccountSettings.HOST)
                .setKey(AccountSettings.MASTER_KEY)
                .setConnectionPolicy(ConnectionPolicy.getDefaultPolicy())
                .setConsistencyLevel(ConsistencyLevel.EVENTUAL)
                .buildAsyncClient();
    }

    public static CosmosAsyncDatabase createNewDatabase(CosmosAsyncClient client, String databaseName) {
        return client.createDatabaseIfNotExists(databaseName).block().getDatabase();
    }

    public static void deleteDatabase(CosmosAsyncDatabase cosmosDatabase) {
        cosmosDatabase.delete().block();
    }

    public static CosmosAsyncContainer createNewCollection(CosmosAsyncClient client, String databaseName, String collectionName, String partitionKey) {
        CosmosAsyncDatabase databaseLink = client.getDatabase(databaseName);
        CosmosAsyncContainer collectionLink = databaseLink.getContainer(collectionName);
        CosmosAsyncContainerResponse containerResponse = null;

        try {
            containerResponse = collectionLink.read().block();

            if (containerResponse != null) {
                throw new IllegalArgumentException(String.format("Collection %s already exists in database %s.", collectionName, databaseName));
            }
        } catch (RuntimeException ex) {
            if (ex instanceof CosmosClientException) {
                CosmosClientException cosmosClientException = (CosmosClientException) ex;

                if (cosmosClientException.getStatusCode() != 404) {
                    throw ex;
                }
            } else {
                throw ex;
            }
        }

        CosmosContainerProperties containerSettings = new CosmosContainerProperties(collectionName, partitionKey);
        CosmosContainerRequestOptions requestOptions = new CosmosContainerRequestOptions();
        containerResponse = databaseLink.createContainer(containerSettings, 10000, requestOptions).block();

        if (containerResponse == null) {
            throw new RuntimeException(String.format("Failed to create collection %s in database %s.", collectionName, databaseName));
        }

        return containerResponse.getContainer();
    }

    public static CosmosAsyncContainer createNewLeaseCollection(CosmosAsyncClient client, String databaseName, String leaseCollectionName) {
        CosmosAsyncDatabase databaseLink = client.getDatabase(databaseName);
        CosmosAsyncContainer leaseCollectionLink = databaseLink.getContainer(leaseCollectionName);
        CosmosAsyncContainerResponse leaseContainerResponse = null;

        try {
            leaseContainerResponse = leaseCollectionLink.read().block();

            if (leaseContainerResponse != null) {
                leaseCollectionLink.delete().block();

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        } catch (RuntimeException ex) {
            if (ex instanceof CosmosClientException) {
                CosmosClientException cosmosClientException = (CosmosClientException) ex;

                if (cosmosClientException.getStatusCode() != 404) {
                    throw ex;
                }
            } else {
                throw ex;
            }
        }

        CosmosContainerProperties containerSettings = new CosmosContainerProperties(leaseCollectionName, "/id");
        CosmosContainerRequestOptions requestOptions = new CosmosContainerRequestOptions();

        leaseContainerResponse = databaseLink.createContainer(containerSettings, 400,requestOptions).block();

        if (leaseContainerResponse == null) {
            throw new RuntimeException(String.format("Failed to create collection %s in database %s.", leaseCollectionName, databaseName));
        }

        return leaseContainerResponse.getContainer();
    }

    public static void createNewDocumentsJSON(CosmosAsyncContainer containerClient, int count, Duration delay) {
        System.out.println("Creating documents\n");
        String suffix = RandomStringUtils.randomAlphabetic(10);
        for (int i = 0; i <= count; i++) {

            String jsonString = "{\"id\" : \"" + String.format("0%d-%s", i, suffix) + "\""
                                 + ","
                                 + "\"brand\" : \"" + ((char)(65+i)) + "\""
                                 + ","
                                 + "\"type\" : \"" + ((char)(69+i)) + "\""
                                 + ","
                                 + "\"expiryDate\" : \"" + "2020-03-" + StringUtils.leftPad(String.valueOf(5+i), 2, "0") + "\""
                                 + "}";

            ObjectMapper mapper = new ObjectMapper();
            JsonNode document = null;

            try {
                document = mapper.readTree(jsonString);
            } catch (Exception e) {
                e.printStackTrace();
            }

            containerClient.createItem(document).subscribe(doc -> {
                System.out.println(".\n");
            });

            long remainingWork = delay.toMillis();
            try {
                while (remainingWork > 0) {
                    Thread.sleep(100);
                    remainingWork -= 100;
                }
            } catch (InterruptedException iex) {
                // exception caught
                break;
            }
        }
    }

    public static boolean ensureWorkIsDone(Duration delay) {
        long remainingWork = delay.toMillis();
        try {
            while (!isWorkCompleted && remainingWork > 0) {
                Thread.sleep(100);
                remainingWork -= 100;
            }
        } catch (InterruptedException iex) {
            return false;
        }

        return remainingWork > 0;
    }

}
