// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.indexmanagement.async;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Families;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.ExcludedPath;
import com.azure.cosmos.models.IncludedPath;
import com.azure.cosmos.models.IndexingMode;
import com.azure.cosmos.models.IndexingPolicy;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedFlux;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class SampleIndexManagementAsync {

    private CosmosAsyncClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    protected static Logger logger = LoggerFactory.getLogger(SampleIndexManagementAsync.class);

    public void close() {
        client.close();
    }

    /**
     * Run a Hello CosmosDB console application.
     * <p>
     * This sample is similar to SampleConflictFeedAsync, but modified to show indexing capabilities of Cosmos DB.
     * Look at the implementation of createContainerIfNotExistsWithSpecifiedIndex() for the demonstration of
     * indexing capabilities.
     */
    //  <Main>
    public static void main(String[] args) {
        SampleIndexManagementAsync p = new SampleIndexManagementAsync();

        try {
            logger.info("Starting ASYNC main");
            p.indexManagementDemo();
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

    private void indexManagementDemo() throws Exception {

        logger.info("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        ArrayList<String> preferredRegions = new ArrayList<String>();
        preferredRegions.add("West US");

        //  Create async client
        //  <CreateAsyncClient>
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .preferredRegions(preferredRegions)
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .contentResponseOnWriteEnabled(true)
                .buildAsyncClient();

        //  </CreateAsyncClient>

        createDatabaseIfNotExists();

        //Here is where index management is performed
        createContainerIfNotExistsWithSpecifiedIndex();

        Family andersenFamilyItem = Families.getAndersenFamilyItem();
        Family wakefieldFamilyItem = Families.getWakefieldFamilyItem();
        Family johnsonFamilyItem = Families.getJohnsonFamilyItem();
        Family smithFamilyItem = Families.getSmithFamilyItem();

        //  Setup family items to create
        Flux<Family> familiesToCreate = Flux.just(andersenFamilyItem,
                wakefieldFamilyItem,
                johnsonFamilyItem,
                smithFamilyItem);

        createFamilies(familiesToCreate);

        familiesToCreate = Flux.just(andersenFamilyItem,
                wakefieldFamilyItem,
                johnsonFamilyItem,
                smithFamilyItem);

        logger.info("Reading items.");
        readItems(familiesToCreate);

        logger.info("Querying items.");
        queryItems();
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

    private void createContainerIfNotExistsWithSpecifiedIndex() throws Exception {
        logger.info("Create container " + containerName + " if not exists.");

        //  Create container if not exists
        //  <CreateContainerIfNotExists>

        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, "/lastName");

        // <CustomIndexingPolicy>
        IndexingPolicy indexingPolicy = new IndexingPolicy();
        indexingPolicy.setIndexingMode(IndexingMode.CONSISTENT); //To turn indexing off set IndexingMode.NONE

        // Included paths
        List<IncludedPath> includedPaths = new ArrayList<>();
        includedPaths.add(new IncludedPath("/*"));
        indexingPolicy.setIncludedPaths(includedPaths);

        // Excluded paths
        List<ExcludedPath> excludedPaths = new ArrayList<>();
        excludedPaths.add(new ExcludedPath("/name/*"));
        indexingPolicy.setExcludedPaths(excludedPaths);

        // Spatial indices - if you need them, here is how to set them up:
        /*
        List<SpatialSpec> spatialIndexes = new ArrayList<SpatialSpec>();
        List<SpatialType> collectionOfSpatialTypes = new ArrayList<SpatialType>();

        SpatialSpec spec = new SpatialSpec();
        spec.setPath("/locations/*");
        collectionOfSpatialTypes.add(SpatialType.Point);
        spec.setSpatialTypes(collectionOfSpatialTypes);
        spatialIndexes.add(spec);

        indexingPolicy.setSpatialIndexes(spatialIndexes);
         */

        // Composite indices - if you need them, here is how to set them up:
        /*
        List<List<CompositePath>> compositeIndexes = new ArrayList<>();
        List<CompositePath> compositePaths = new ArrayList<>();

        CompositePath nameCompositePath = new CompositePath();
        nameCompositePath.setPath("/name");
        nameCompositePath.setOrder(CompositePathSortOrder.ASCENDING);

        CompositePath ageCompositePath = new CompositePath();
        ageCompositePath.setPath("/age");
        ageCompositePath.setOrder(CompositePathSortOrder.DESCENDING);

        compositePaths.add(ageCompositePath);
        compositePaths.add(nameCompositePath);

        compositeIndexes.add(compositePaths);
        indexingPolicy.setCompositeIndexes(compositeIndexes);
         */

        containerProperties.setIndexingPolicy(indexingPolicy);

        // </CustomIndexingPolicy>

        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);
        Mono<CosmosContainerResponse> containerIfNotExists = database.createContainerIfNotExists(containerProperties, throughputProperties);

        //  Create container with 400 RU/s
        containerIfNotExists.flatMap(containerResponse -> {
            container = database.getContainer(containerResponse.getProperties().getId());
            logger.info("Checking container " + container.getId() + " completed!\n");
            return Mono.empty();
        }).block();

        //  </CreateContainerIfNotExists>
    }

    private void createFamilies(Flux<Family> families) throws Exception {

        //  <CreateItem>

        final CountDownLatch completionLatch = new CountDownLatch(1);

        //  Combine multiple item inserts, associated success println's, and a final aggregate stats println into one Reactive stream.
        families.flatMap(family -> {
            return container.createItem(family);
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
                .subscribe(charge -> {
                            logger.info(String.format("Created items with total request charge of %.2f\n",
                                    charge));
                        },
                        err -> {
                            if (err instanceof CosmosException) {
                                //Client-specific errors
                                CosmosException cerr = (CosmosException) err;
                                cerr.printStackTrace();
                                logger.info(String.format("Read Item failed with %s\n", cerr));
                            } else {
                                //General errors
                                err.printStackTrace();
                            }

                            completionLatch.countDown();
                        },
                        () -> {
                            completionLatch.countDown();
                        }
                ); //Preserve the total charge and print aggregate charge/item count stats.

        try {
            completionLatch.await();
        } catch (InterruptedException err) {
            throw new AssertionError("Unexpected Interruption", err);
        }

        //  </CreateItem>
    }

    private void readItems(Flux<Family> familiesToCreate) {
        //  Using partition key for point read scenarios.
        //  This will help fast look up of items because of partition key
        //  <ReadItem>

        final CountDownLatch completionLatch = new CountDownLatch(1);

        familiesToCreate.flatMap(family -> {
            Mono<CosmosItemResponse<Family>> asyncItemResponseMono = container.readItem(family.getId(), new PartitionKey(family.getLastName()), Family.class);
            return asyncItemResponseMono;
        })
                .subscribe(
                        itemResponse -> {
                            double requestCharge = itemResponse.getRequestCharge();
                            Duration requestLatency = itemResponse.getDuration();
                            logger.info(String.format("Item successfully read with id %s with a charge of %.2f and within duration %s",
                                    itemResponse.getItem().getId(), requestCharge, requestLatency));
                        },
                        err -> {
                            if (err instanceof CosmosException) {
                                //Client-specific errors
                                CosmosException cerr = (CosmosException) err;
                                cerr.printStackTrace();
                                logger.info(String.format("Read Item failed with %s\n", cerr));
                            } else {
                                //General errors
                                err.printStackTrace();
                            }

                            completionLatch.countDown();
                        },
                        () -> {
                            completionLatch.countDown();
                        }
                );

        try {
            completionLatch.await();
        } catch (InterruptedException err) {
            throw new AssertionError("Unexpected Interruption", err);
        }

        //  </ReadItem>
    }

    private void queryItems() {
        //  <QueryItems>

        // Set some common query options
        int preferredPageSize = 10;
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        //  Set populate query metrics to get metrics around query executions
        queryOptions.setQueryMetricsEnabled(true);

        CosmosPagedFlux<Family> pagedFluxResponse = container.queryItems(
                "SELECT * FROM Family WHERE Family.lastName IN ('Andersen', 'Wakefield', 'Johnson')", queryOptions, Family.class);

        final CountDownLatch completionLatch = new CountDownLatch(1);

        pagedFluxResponse.byPage(preferredPageSize).subscribe(
                fluxResponse -> {
                    logger.info("Got a page of query result with " +
                            fluxResponse.getResults().size() + " items(s)"
                            + " and request charge of " + fluxResponse.getRequestCharge());

                    logger.info("Item Ids " + fluxResponse
                            .getResults()
                            .stream()
                            .map(Family::getId)
                            .collect(Collectors.toList()));
                },
                err -> {
                    if (err instanceof CosmosException) {
                        //Client-specific errors
                        CosmosException cerr = (CosmosException) err;
                        cerr.printStackTrace();
                        logger.error(String.format("Read Item failed with %s\n", cerr));
                    } else {
                        //General errors
                        err.printStackTrace();
                    }

                    completionLatch.countDown();
                },
                () -> {
                    completionLatch.countDown();
                }
        );

        try {
            completionLatch.await();
        } catch (InterruptedException err) {
            throw new AssertionError("Unexpected Interruption", err);
        }

        // </QueryItems>
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
