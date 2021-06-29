// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.subpartitioning;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.UserSession;
import com.azure.cosmos.examples.common.UserSessionData;
import com.azure.cosmos.examples.crudquickstart.async.SampleCRUDQuickstartAsync;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerRequestOptions;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.PartitionKeyBuilder;
import com.azure.cosmos.models.PartitionKeyDefinition;
import com.azure.cosmos.models.PartitionKeyDefinitionVersion;
import com.azure.cosmos.models.PartitionKind;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedFlux;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SampleSubpartitioningAsync {
    private CosmosAsyncClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    protected static Logger logger = LoggerFactory.getLogger(SampleCRUDQuickstartAsync.class);

    public void close() {
        client.close();
    }

    /**
     * Run a Hello CosmosDB console application.
     * <p>
     * This is a simple sample application intended to demonstrate Create, Read, Update, Delete (CRUD) operations
     * with Azure Cosmos DB Java SDK, as applied to databases, containers and items. This sample will
     * 1. Create asynchronous client, database and container instance with multiple partition key paths
     * 2. Create several items
     * 3. Upsert one of the items
     * 4. Perform a query over the items
     * 5. Delete an item
     * 6. Delete the Cosmos DB database and container resources and close the client.
     */
    //  <Main>
    public static void main(String[] args) {
        SampleSubpartitioningAsync p = new SampleSubpartitioningAsync();
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

    private void getStartedDemo() throws Exception {

        logger.info("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        ArrayList<String> preferredRegions = new ArrayList<String>();
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
                .buildAsyncClient();

        //  </CreateAsyncClient>

        createDatabaseIfNotExists();
        createContainerIfNotExists();

        //  Setup UserSession items to create
        List<UserSession> userSessions = UserSessionData.buildSampleSessionData();
        Flux<UserSession> familiesToCreate = Flux.fromIterable(userSessions);

        // Creates several items in the container
        createFamilies(familiesToCreate);

        logger.info("Reading items.");
        readItems(familiesToCreate);

        logger.info("Querying items.");
        queryItems();

        logger.info("Deleting an item.");
        deleteItem(userSessions.get(0));
    }

    private void createDatabaseIfNotExists() throws Exception {
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

    private void createContainerIfNotExists() throws Exception {
        logger.info("Create container " + containerName + " if not exists.");

        //  Create container if not exists
        // <Create PartitionKeyDefinition>
        List<String> partitionKeyPaths = new ArrayList<String>();
        partitionKeyPaths.add("/tenantId");
        partitionKeyPaths.add("/userId");
        partitionKeyPaths.add("/sessionId");
        PartitionKeyDefinition subpartitionKeyDefinition = new PartitionKeyDefinition();
        subpartitionKeyDefinition.setPaths(partitionKeyPaths);
        subpartitionKeyDefinition.setKind(PartitionKind.MULTI_HASH);
        subpartitionKeyDefinition.setVersion(PartitionKeyDefinitionVersion.V2);
        //  <CreateContainerIfNotExists>
        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, subpartitionKeyDefinition);
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);
        Mono<CosmosContainerResponse> containerIfNotExists = database.createContainerIfNotExists(containerProperties, throughputProperties);

        //  Create container with 400 RU/s
        CosmosContainerResponse cosmosContainerResponse = containerIfNotExists.block();
        container = database.getContainer(cosmosContainerResponse.getProperties().getId());
        //  </CreateContainerIfNotExists>

        //Modify existing container
        containerProperties = cosmosContainerResponse.getProperties();
        Mono<CosmosContainerResponse> propertiesReplace = container.replace(containerProperties, new CosmosContainerRequestOptions());
        propertiesReplace.flatMap(containerResponse -> {
            logger.info("setupContainer(): Container " + container.getId() + " in " + database.getId() +
                    "has been updated with it's new properties.");
            return Mono.empty();
        }).onErrorResume((exception) -> {
            logger.error("setupContainer(): Unable to update properties for container " + container.getId() +
                    " in database " + database.getId() +
                    ". e: " + exception.getLocalizedMessage());
            return Mono.empty();
        }).block();

    }

    private void createFamilies(Flux<UserSession> userSessions) throws Exception {

        //  <CreateItem>

        try {

            //  Combine multiple item inserts, associated success println's, and a final aggregate stats println into one Reactive stream.
            double charge = userSessions.flatMap(userSession -> {
                return container.createItem(userSession);
            }) //Flux of item request responses
                    .flatMap(itemResponse -> {
                        logger.info(String.format("Created item with request charge of %.2f within" +
                                        " duration %s",
                                itemResponse.getRequestCharge(), itemResponse.getDuration()));
                        logger.info(String.format("Item ID: %s\n", itemResponse.getItem().getId()));
                        return Mono.just(itemResponse.getRequestCharge());
                    }) //Flux of request charges
                    .reduce(0.0,
                            (charge_n, charge_nplus1) -> charge_n + charge_nplus1
                    ) //Mono of total charge - there will be only one item in this stream
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

    private void readItems(Flux<UserSession> userSessionFlux) {
        //  Using partition key for point read scenarios.
        //  This will help fast look up of items because of partition key
        //  <ReadItem>

        try {

            userSessionFlux.flatMap(userSession -> {
                Mono<CosmosItemResponse<UserSession>> asyncItemResponseMono = container.readItem(userSession.getId(),
                        new PartitionKeyBuilder()
                                .add(userSession.getTenantId())
                                .add(userSession.getUserId())
                                .add(userSession.getSessionId())
                                .build()
                        , UserSession.class);
                return asyncItemResponseMono;
            }).flatMap(itemResponse -> {
                double requestCharge = itemResponse.getRequestCharge();
                Duration requestLatency = itemResponse.getDuration();
                logger.info(String.format("Item successfully read with id %s with a charge of %.2f and within duration %s",
                        itemResponse.getItem().getId(), requestCharge, requestLatency));
                return Flux.empty();
            }).blockLast();

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

        //  </ReadItem>
    }
// Add the
    private void queryItems() {
        //  <QueryItems>
        // Set some common query options

        int preferredPageSize = 10; // We'll use this later

        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();

        //  Set populate query metrics to get metrics around query executions
        queryOptions.setQueryMetricsEnabled(true);
        /**  Subpartitioning support:
         *      Specifying partial partition key values in query, will only route the query to
         *      the subset of physical partitions on which the documents with this partition key
         *      value exist.
        **/
         CosmosPagedFlux<UserSession> pagedFluxResponse = container.queryItems(
                "SELECT * FROM t WHERE t.TenantId IN ('Microsoft')", queryOptions, UserSession.class);

        try {

            pagedFluxResponse.byPage(preferredPageSize).flatMap(fluxResponse -> {
                logger.info("Got a page of query result with " +
                        fluxResponse.getResults().size() + " items(s)"
                        + " and request charge of " + fluxResponse.getRequestCharge());

                logger.info("Item Ids " + fluxResponse
                        .getResults()
                        .stream()
                        .map(UserSession::getId)
                        .collect(Collectors.toList()));

                return Flux.empty();
            }).blockLast();

        } catch(Exception err) {
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

        // </QueryItems>
    }

    private void deleteItem(UserSession item) {
        container.deleteItem(item.getId(), new PartitionKeyBuilder().add(item.getTenantId()).add(item.getUserId()).add(item.getSessionId()).build()).block();
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

}
