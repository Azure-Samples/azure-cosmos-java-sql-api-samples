// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.documentcrud.async;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Families;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseRequestOptions;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
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

    private boolean replaceDone;
    private boolean upsertDone;
    private String etag;

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
            logger.error(String.format("Cosmos getStarted failed with %s", e));
        } finally {
            logger.info("Closing the client");
            p.shutdown();
        }
    }

    private void documentCRUDDemo() throws Exception {

        logger.info("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        //  Create sync client
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .contentResponseOnWriteEnabled(true)
                .buildAsyncClient();


        createDatabaseIfNotExists();
        createContainerIfNotExists();
        
        //add docs to array
        List<Family> families = new ArrayList<>();
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

        readDocumentById();
        //readAllDocumentsInContainer(); <Deprecated>
        queryDocuments();
        getDocumentsAsJsonArray();
        replaceDocument();
        upsertDocument();
        while(!(upsertDone && replaceDone)){
            logger.info("checking for async upsert and replace to complete...");
            Thread.sleep(1);
        }
        logger.info("replace and upsert done now...");
        replaceDocumentWithConditionalEtagCheck();
        readDocumentOnlyIfChanged();
        // deleteDocument() is called at shutdown()

    }

    // Database Create
    private void createDatabaseIfNotExists() throws Exception {
        logger.info("Create database " + databaseName + " if not exists...");

        //  Create database if not exists
        Mono<CosmosDatabaseResponse> databaseResponse = client.createDatabaseIfNotExists(databaseName);
        database = client.getDatabase(databaseResponse.block().getProperties().getId());

        logger.info("Done.");
    }

    // Container create
    private void createContainerIfNotExists() throws Exception {
        logger.info("Create container " + containerName + " if not exists.");

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
        logger.info("Create document " + documentId);

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
                    logger.info("inserted doc with id: "+response.getItem().getId());
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
        logger.info("Create documents " + documentId);

        // Define a document as a POJO (internally this
        // is converted to JSON via custom serialization)
        // Combine multiple item inserts, associated success println's, and a final
        // aggregate stats println into one Reactive stream.
        double charge = families.flatMap(family -> {
            return container.createItem(family);
        }) // Flux of item request responses
                .flatMap(itemResponse -> {
                    logger.info(String.format("Created item with request charge of %.2f within" +
                            " duration %s",
                            itemResponse.getRequestCharge(), itemResponse.getDuration()));
                    logger.info(String.format("Item ID: %s\n", itemResponse.getItem().getId()));
                    return Mono.just(itemResponse.getRequestCharge());
                }) // Flux of request charges
                .reduce(0.0,
                        (charge_n, charge_nplus1) -> charge_n + charge_nplus1) // Mono of total charge - there will be
                                                                               // only one item in this stream
                .doOnSuccess(itemResponse -> {
                    logger.info("Aggregated charge (all decimals): "+itemResponse);
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                })
                .block(); // Preserve the total charge and print aggregate charge/item count stats.

        logger.info(String.format("Created items with total request charge of %.2f\n", charge));
    }   

    // Document read
    private void readDocumentById() throws Exception {
        logger.info("Read document " + documentId + " by ID.");

        //  Read document by ID
        CosmosItemResponse<Family> family = container.readItem(documentId,new PartitionKey(documentLastName),Family.class)
        .doOnSuccess((response) -> {
            try {
                logger.info("item converted to json: "+new ObjectMapper().writeValueAsString(response.getItem()));
            } catch (JsonProcessingException e) {
                logger.error("Exception processing json: "+e);
            }  
        })      
        .doOnError(Exception.class, exception -> {
            logger.error(
                    "Exception. e: {}",
                    exception.getLocalizedMessage(),
                    exception);
        }).block();

        // Check result
        logger.info("Finished reading family " + family.getItem().getId() + " with partition key " + family.getItem().getLastName());
        logger.info("Done.");
    }

    private void queryDocuments() throws Exception {
        logger.info("Query documents in the container " + containerName + ".");

        String sql = "SELECT * FROM c WHERE c.lastName = 'Witherspoon'";

        CosmosPagedFlux<Family> filteredFamilies = container.queryItems(sql, new CosmosQueryRequestOptions(), Family.class);

        // Print
        filteredFamilies.byPage(100).flatMap(filteredFamiliesFeedResponse -> {
            logger.info("Got a page of query result with " +
            filteredFamiliesFeedResponse.getResults().size() + " items(s)"
                    + " and request charge of " + filteredFamiliesFeedResponse.getRequestCharge());
            for (Family family : filteredFamiliesFeedResponse.getResults()) {
                logger.info("First query result: Family with (/id, partition key) = (%s,%s)",family.getId(),family.getLastName());
            }            
            return Flux.empty();
        }).blockLast();        
        logger.info("Done.");
    }

    private void getDocumentsAsJsonArray() throws Exception {
        logger.info("Query documents in the container " + containerName + ".");
        int preferredPageSize = 10;
        String sql = "SELECT * FROM c WHERE c.lastName = 'Witherspoon'";
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        queryOptions.setQueryMetricsEnabled(true);
        CosmosPagedFlux<JsonNode> pagedFlux = container.queryItems(sql, queryOptions,
                JsonNode.class);
        List<JsonNode> jsonList = pagedFlux.byPage(preferredPageSize)
                .flatMap(pagedFluxResponse -> {
                    // collect all documents in reactive stream to a list of JsonNodes
                    return Flux.just(pagedFluxResponse
                            .getResults()
                            .stream()
                            .collect(Collectors.toList()));
                }).onErrorResume((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                    return Mono.empty();
                }).blockLast();
        //convert to ArrayNode
        logger.info("docs as json array: " + new ArrayNode(JsonNodeFactory.instance, jsonList).toString());
    }

    private void replaceDocument() throws Exception {
        logger.info("Replace document " + documentId);

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
                    replaceDone = true;
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
        logger.info("Replace document " + documentId);

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
                    upsertDone = true;
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
        logger.info("Replace document " + documentId + ", employing optimistic concurrency using ETag.");

        // Obtained current document ETag
        CosmosItemResponse<Family> famResp =
                container.readItem(documentId, new PartitionKey(documentLastName), Family.class)                
                .doOnSuccess(itemResponse -> {
                    logger.info("Request charge of readItem operation: {} RU", itemResponse.getRequestCharge());                  
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).block(); 
               
        String etag = famResp.getResponseHeaders().get("etag");
        logger.info("Read document " + documentId + " to obtain current ETag: " + etag);

        // Modify document
        Family family = famResp.getItem();
        family.setRegistered(!family.isRegistered());

        // Persist the change back to the server, updating the ETag in the process
        // This models a concurrent change made to the document
        CosmosItemResponse<Family> updatedFamResp =
                container.replaceItem(family,family.getId(),new PartitionKey(family.getLastName()),new CosmosItemRequestOptions())                
                .doOnSuccess(itemResponse -> {
                    logger.info("'Concurrent' update to document " + documentId + " so ETag is now " + itemResponse.getResponseHeaders().get("etag"));
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).block(); 
        

        // Now update the document and call replace with the AccessCondition requiring that ETag has not changed.
        // This should fail because the "concurrent" document change updated the ETag.
      
        CosmosItemRequestOptions requestOptions = new CosmosItemRequestOptions();
        requestOptions.setIfMatchETag(etag);

        family.setDistrict("Seafood");

        container.replaceItem(family, family.getId(), new PartitionKey(family.getLastName()), requestOptions)
                .doOnSuccess(itemResponse -> {
                    logger.info("Request charge of replace operation: {} RU", itemResponse.getRequestCharge());
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                            // no need to block here so will execute in the background
                }).subscribe();

        logger.info("final replace with conditional etag check done asynchronously...");
    }

    private void readDocumentOnlyIfChanged() throws Exception {
        logger.info("Read document " + documentId + " only if it has been changed, utilizing an ETag check.");

        // Read document (we'll make this one async)
        container.readItem(documentId, new PartitionKey(documentLastName), Family.class)
                .doOnSuccess(itemResponse -> {
                    logger.info("Read doc with status code of {}", itemResponse.getStatusCode());
                    //assign etag to the instance variable asynchronously
                    etag = itemResponse.getResponseHeaders().get("etag");;
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).subscribe();

        while (this.etag == null) {
            logger.info("waiting until we got the etag from the first read....");
            Thread.sleep(1);
        }
        //etag retrieved from first read so we can safely assign to instance variable
        String etag = this.etag;

        // Re-read doc but with conditional access requirement that ETag has changed.
        // This should fail.
        CosmosItemRequestOptions requestOptions = new CosmosItemRequestOptions();
        requestOptions.setIfNoneMatchETag(etag);

        //blocking on this call
        CosmosItemResponse<Family> famResp = container
                .readItem(documentId, new PartitionKey(documentLastName), requestOptions, Family.class)
                .doOnSuccess(itemResponse -> {
                    logger.info(
                            "Re-read doc with status code of {} (we anticipate failure due to ETag not having changed.)",
                            itemResponse.getStatusCode());
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).block();


        // Replace the doc with a modified version, which will update ETag (blocking on this call)
        Family family = famResp.getItem();
        family.setRegistered(!family.isRegistered());
        container.replaceItem(family,family.getId(),new PartitionKey(family.getLastName()),new CosmosItemRequestOptions())                
                .doOnSuccess(itemResponse -> {
                    logger.info("Request charge of replace operation: {} RU", itemResponse.getRequestCharge());
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).block();
                
                logger.info("Modified and replaced the doc (updates ETag.)");

        // Re-read doc again, with conditional acccess requirements.
        // This should succeed since ETag has been updated.
        // no need to block here, so we'll fire and forget (subscribe)
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
        logger.info("Delete document " + documentId + " by ID.");

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
        logger.info("Last step: delete database " + databaseName + " by ID.");

        // Delete database
        client.getDatabase(databaseName).delete(new CosmosDatabaseRequestOptions())        .doOnSuccess(itemResponse -> {
            logger.info("Request charge of delete database operation: {} RU", itemResponse.getRequestCharge());
            logger.info("Status code for database delete: {}",itemResponse.getStatusCode());
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
