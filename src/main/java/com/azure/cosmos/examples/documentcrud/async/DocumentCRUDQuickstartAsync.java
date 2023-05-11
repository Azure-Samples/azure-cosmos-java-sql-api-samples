// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.documentcrud.async;

import com.azure.cosmos.*;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Families;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseRequestOptions;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class DocumentCRUDQuickstartAsync {

    private CosmosAsyncClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";
    private final String documentId = UUID.randomUUID().toString();
    private final String documentLastName = "Witherspoon";

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    private List<JsonNode> jsonList;
    private String etag1;
    private String etag2;


    Family family = new Family();
    Family family2 = new Family();

    List<Family> families = new ArrayList<>();

    protected static Logger logger = LoggerFactory.getLogger(DocumentCRUDQuickstartAsync.class);

    public void close() {
        client.close();
    }

    /**
     * Sample to demonstrate the following document CRUD operations:
     * -Create
     * -Read by ID
     * -Read all
     * -Query
     * -Replace
     * -Upsert
     * -Replace with conditional ETag check
     * -Read document only if document has changed
     * -Delete
     */
    public static void main(String[] args) {
        DocumentCRUDQuickstartAsync p = new DocumentCRUDQuickstartAsync();
        try {
            logger.info("Starting ASYNC main");
            p.documentCRUDDemo();
            logger.info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Cosmos getStarted failed with {}", e);
        } finally {
            logger.info("Closing the client");
            p.shutdown();
        }
    }

    private void documentCRUDDemo() throws Exception {

        logger.info("Using Azure Cosmos DB endpoint: {}", AccountSettings.HOST);

        //  Create async client
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .contentResponseOnWriteEnabled(true)
                .buildAsyncClient();


        createDatabaseIfNotExists();
        createContainerIfNotExists();

        //add docs to array
        
        families.add(Families.getAndersenFamilyItem());
        families.add(Families.getWakefieldFamilyItem());
        families.add(Families.getJohnsonFamilyItem());
        families.add(Families.getSmithFamilyItem());

        // add array of docs to reactive stream
        Flux<Family> familiesToCreate = Flux.fromIterable(families);

        //this will insert a hardcoded doc
        createDocument();

        //this will insert the docs in the reactive stream
        createDocuments(familiesToCreate);

        // We are adding Thread.sleep to mimic the some business computation that can
        // happen while waiting for earlier processes to finish.
        Thread.sleep(1000);

        //done async
        readDocumentById();
        //readAllDocumentsInContainer(); <Deprecated>

        //done async
        queryDocuments();

        getDocumentsAsJsonArray();

        // We are adding Thread.sleep to mimic the some business computation that can
        // happen while waiting for earlier processes to finish.
        Thread.sleep(1000);

        //convert jsonList to ArrayNode
        ArrayNode jsonArray = new ArrayNode(JsonNodeFactory.instance, this.jsonList);
        logger.info("docs as json array: {}", jsonArray.toString());
        replaceDocument();
        upsertDocument();

        // We are adding Thread.sleep to mimic the some business computation that can
        // happen while waiting for earlier processes to finish.
        Thread.sleep(1000);

        logger.info("replace and upsert done now...");
        replaceDocumentWithConditionalEtagCheck();
        readDocumentOnlyIfChanged();
        // deleteDocument() is called at shutdown()

    }

    // Database Create
    private void createDatabaseIfNotExists() throws Exception {
        logger.info("Create database {} if not exists...", databaseName);

        //  Create database if not exists
        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName).block();
        database = client.getDatabase(databaseResponse.getProperties().getId());

        logger.info("Done.");
    }

    // Container create
    private void createContainerIfNotExists() throws Exception {
        logger.info("Create container {} if not exists.", containerName);

        // Create container if not exists - this is async but we block to make sure
        // database and containers are created before sample runs the CRUD operations on
        // them
        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(containerName, "/lastName");

        // Provision throughput
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);

        //  Create container with 200 RU/s
        CosmosContainerResponse containerResponse = database.createContainerIfNotExists(containerProperties, throughputProperties).block();
        container = database.getContainer(containerResponse.getProperties().getId());

        logger.info("Done.");
    }

    private void createDocument() throws Exception {
        logger.info("Create document {}", documentId);

        // Define a document as a POJO (internally this
        // is converted to JSON via custom serialization)

        Family family = new Family();
        family.setLastName(documentLastName);
        family.setId(documentId);

        // Insert this item as a document
        // Explicitly specifying the /pk value improves performance.
        // add subscribe() to make this async
        container.createItem(family, new PartitionKey(family.getLastName()), new CosmosItemRequestOptions())
                .doOnSuccess((response) -> {
                    logger.info("inserted doc with id: {}", response.getItem().getId());
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).subscribe();
        logger.info("creating doc asynchronously...");
    }

    private void createDocuments(Flux<Family> families) throws Exception {
        logger.info("Create documents {}", this.families.stream().map(x -> x.getId()).collect(Collectors.toList()));

        // Define a document as a POJO (internally this
        // is converted to JSON via custom serialization)
        // Combine multiple item inserts, associated success println's, and a final
        // aggregate stats println into one Reactive stream.
        families.flatMap(family -> {
                    return container.createItem(family);
                }) // Flux of item request responses
                .flatMap(itemResponse -> {
                    logger.info("Created item with request charge of {} within duration {}", itemResponse.getRequestCharge(), itemResponse.getDuration());
                    logger.info("Item ID: {}\n", itemResponse.getItem().getId());
                    return Mono.just(itemResponse.getRequestCharge());
                }) // Flux of request charges
                .reduce(0.0,
                        (charge_n, charge_nplus1) -> charge_n + charge_nplus1) // Mono of total charge - there will be
                // only one item in this stream
                .doOnSuccess(itemResponse -> {
                    logger.info("Aggregated charge (all decimals): {}", itemResponse);
                    // Preserve the total charge and print aggregate charge/item count stats.
                    logger.info("Created items with total request charge of {}\n", itemResponse);
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                })
                .subscribe();
    }

    // Document read
    private void readDocumentById() throws Exception {
        logger.info("Read document {} by ID.", documentId);

        //  Read document by ID
        container.readItem(documentId, new PartitionKey(documentLastName), Family.class)
                .doOnSuccess((response) -> {
                    try {
                        logger.info("item converted to json: {}", new ObjectMapper().writeValueAsString(response.getItem()));
                        logger.info("Finished reading family {} with partition key {}", response.getItem().getId(), response.getItem().getLastName());
                    } catch (JsonProcessingException e) {
                        logger.error("Exception processing json: {}", e);
                    }
                })
                .doOnError(Exception.class, exception -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).subscribe();

        logger.info("readDocumentById done asynchronously...");
    }

    private void queryDocuments() throws Exception {
        logger.info("Query documents in the container {}.", containerName);

        String sql = "SELECT * FROM c WHERE c.lastName = 'Witherspoon'";

        CosmosPagedFlux<Family> filteredFamilies = container.queryItems(sql, new CosmosQueryRequestOptions(), Family.class);

        // Print
        filteredFamilies.byPage(100).flatMap(filteredFamiliesFeedResponse -> {
            logger.info("Got a page of query result with {} items(s) and request charge of {}", filteredFamiliesFeedResponse.getResults().size(), filteredFamiliesFeedResponse.getRequestCharge());
            for (Family family : filteredFamiliesFeedResponse.getResults()) {
                logger.info("First query result: Family with (/id, partition key) = ({},{})", family.getId(), family.getLastName());
            }

            return Flux.empty();
        }).subscribe();
        logger.info("queryDocuments done asynchronously.");
    }

    private void getDocumentsAsJsonArray() throws Exception {
        logger.info("Query documents in the container {}.", containerName);
        int preferredPageSize = 10;
        String sql = "SELECT * FROM c WHERE c.lastName = 'Witherspoon'";
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        queryOptions.setQueryMetricsEnabled(true);
        CosmosPagedFlux<JsonNode> pagedFlux = container.queryItems(sql, queryOptions,
                JsonNode.class);
        pagedFlux.byPage(preferredPageSize)
                .flatMap(pagedFluxResponse -> {
                    // collect all documents in reactive stream to a list of JsonNodes
                    return Flux.just(pagedFluxResponse
                            .getResults()
                            .stream()
                            .collect(Collectors.toList()));
                })
                .onErrorResume((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                    return Mono.empty();
                }).subscribe(listJsonNode -> {
                    //pass the List<JsonNode> to the instance variable
                    this.jsonList = listJsonNode;
                });

    }

    private void replaceDocument() throws Exception {
        logger.info("Replace document {}", documentId);

        // Replace existing document with new modified document
        Family family = new Family();
        family.setLastName(documentLastName);
        family.setId(documentId);
        family.setDistrict("Columbia"); // Document modification

        container.replaceItem(family, family.getId(),
                        new PartitionKey(family.getLastName()), new CosmosItemRequestOptions())
                .doOnSuccess(itemResponse -> {
                    logger.info("Request charge of replace operation: {} RU", itemResponse.getRequestCharge());
                    //set flag so calling method can know when done
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).subscribe();
        logger.info("replace subscribed: will process asynchronously....");
    }

    private void upsertDocument() throws Exception {
        logger.info("Replace document {}", documentId);

        // Replace existing document with new modified document (contingent on
        // modification).
        Family family = new Family();
        family.setLastName(documentLastName);
        family.setId(documentId);
        family.setDistrict("Columbia"); // Document modification

        container.upsertItem(family, new CosmosItemRequestOptions())
                .doOnSuccess(itemResponse -> {
                    logger.info("Request charge of upsert operation: {} RU", itemResponse.getRequestCharge());
                    // set flag so calling method can know when done
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).subscribe();
        logger.info("upsert subscribed: will process asynchronously....");
    }

    private void replaceDocumentWithConditionalEtagCheck() throws Exception {
        logger.info("Replace document {}, employing optimistic concurrency using ETag.", documentId);

        //chain reading and replacing of item together in reactive stream
        container.readItem(documentId, new PartitionKey(documentLastName), Family.class)
                .flatMap(response -> { 
                    logger.info("Read document {} to obtain current ETag: {}", documentId, response.getETag());
                    //set instance variables for family nd etag1 to use later
                    this.family = response.getItem();
                    this.etag1 = response.getETag();
                    logger.info("Request charge of readItem operation: {} RU", response.getRequestCharge());       
                    Family family = this.family;
                    family.setRegistered(!family.isRegistered());
                    // Persist the change back to the server, updating the ETag in the process
                    // This models a concurrent change made to the document
                    return container
                            .replaceItem(family, family.getId(), new PartitionKey(family.getLastName()),
                                    new CosmosItemRequestOptions())
                            .doOnSuccess(itemResponse -> {
                                logger.info("'Concurrent' update to document {} so ETag is now {}", documentId,
                                        itemResponse.getResponseHeaders().get("etag"));
                            })
                            .doOnError((exception) -> {
                                logger.error(
                                        "Exception e: {}",
                                        exception.getLocalizedMessage(),
                                        exception);
                            });
                }).subscribe();

        // We are adding Thread.sleep to mimic the some business computation that can
        // happen while waiting for earlier processes to finish.
        Thread.sleep(1000);

        // Now update the document and call replace with the AccessCondition requiring that ETag has not changed.
        // This should fail because the "concurrent" document change updated the ETag.

        CosmosItemRequestOptions requestOptions = new CosmosItemRequestOptions();
        //using etag1 instance variable saved from earlier update to etag
        requestOptions.setIfMatchETag(this.etag1.toString());

        family.setDistrict("Seafood");

        container.replaceItem(family, family.getId(), new PartitionKey(family.getLastName()), requestOptions)
                .doOnSuccess(itemResponse -> {
                    logger.info("Request charge of replace operation: {} RU", itemResponse.getRequestCharge());
                })
                .doOnError((exception) -> {
                    logger.info("As expected, we have a pre-condition failure exception\n");
                })
                .onErrorResume(exception -> Mono.empty())
                .subscribe();
    }

    private void readDocumentOnlyIfChanged() throws Exception {
        logger.info("Read document {} only if it has been changed, utilizing an ETag check.", documentId);

        // Read document
        container.readItem(documentId, new PartitionKey(documentLastName), Family.class)
                .doOnSuccess(itemResponse -> {
                    logger.info("Read doc with status code of {}", itemResponse.getStatusCode());
                    //assign etag to the instance variable asynchronously
                    this.family2 = itemResponse.getItem();
                    this.etag2 = itemResponse.getResponseHeaders().get("etag");
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).subscribe();

        // We are adding Thread.sleep to mimic the some business computation that can
        // happen while waiting for earlier processes to finish.
        Thread.sleep(1000);

        //etag retrieved from first read so we can safely assign to instance variable
        String etag = this.etag2;

        // Re-read doc but with conditional access requirement that ETag has changed.
        // This should fail (304).
        CosmosItemRequestOptions requestOptions = new CosmosItemRequestOptions();
        requestOptions.setIfNoneMatchETag(etag);

        //chain reading and replacing of item together in reactive stream
        container.readItem(documentId, new PartitionKey(documentLastName), requestOptions, Family.class)
        .flatMap(response -> { 
            logger.info("Re-read doc with status code of {} (we anticipate 304 failure due to ETag not having changed.)", response.getStatusCode());
            logger.info("Request charge of readItem operation: {} RU", response.getRequestCharge());       
            Family family = this.family2;
            family.setRegistered(!family.isRegistered());
            // Replace the doc with a modified version, which will update ETag
            return container
                    .replaceItem(family, family.getId(), new PartitionKey(family.getLastName()),
                            new CosmosItemRequestOptions())
                    .doOnSuccess(itemResponse -> {
                        logger.info("'Concurrent' update to document {} so ETag is now {}", documentId,
                                itemResponse.getResponseHeaders().get("etag"));
                    })
                    .doOnError((exception) -> {
                        logger.error(
                                "Exception e: {}",
                                exception.getLocalizedMessage(),
                                exception);
                    });
        }).subscribe();

        // We are adding Thread.sleep to mimic the some business computation that can
        // happen while waiting for earlier processes to finish.
        Thread.sleep(1000);

        // Re-read doc again, with conditional access requirements.
        // This should succeed since ETag has been updated.
        container.readItem(documentId, new PartitionKey(documentLastName), requestOptions, Family.class)
                .doOnSuccess(itemResponse -> {
                    logger.info("Re-read doc with status code of {} (we anticipate success due to ETag modification.)", itemResponse.getStatusCode());
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).subscribe();

        logger.info("final etag check will be done async...");
    }

    // Document delete
    private void deleteADocument() throws Exception {
        logger.info("Delete document {} by ID.", documentId);

        // Delete document
        container.deleteItem(documentId, new PartitionKey(documentLastName), new CosmosItemRequestOptions())
                .doOnSuccess(itemResponse -> {
                    logger.info("Request charge of delete document operation: {} RU", itemResponse.getRequestCharge());
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).block();

        logger.info("Done.");
    }

    // Database delete
    private void deleteADatabase() throws Exception {
        logger.info("Last step: delete database {} by ID.", databaseName);

        // Delete database
        client.getDatabase(databaseName).delete(new CosmosDatabaseRequestOptions()).doOnSuccess(itemResponse -> {
                    logger.info("Request charge of delete database operation: {} RU", itemResponse.getRequestCharge());
                    logger.info("Status code for database delete: {}", itemResponse.getStatusCode());
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).block();

        logger.info("Done.");
    }

    // Cleanup before close
    private void shutdown() {
        try {
            //Clean shutdown
            deleteADocument();
            deleteADatabase();
        } catch (Exception err) {
            logger.error("Deleting Cosmos DB resources failed, will still attempt to close the client. See stack trace below.");
            err.printStackTrace();
        }
        client.close();
        logger.info("Done with sample.");
    }

}
