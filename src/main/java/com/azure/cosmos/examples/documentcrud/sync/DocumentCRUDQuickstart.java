// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.documentcrud.sync;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.examples.common.AccountSettings;
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
import com.azure.cosmos.util.CosmosPagedIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class DocumentCRUDQuickstart {

    private CosmosClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";
    private final String documentId = UUID.randomUUID().toString();
    private final String documentLastName = "Witherspoon";

    private CosmosDatabase database;
    private CosmosContainer container;

    protected static Logger logger = LoggerFactory.getLogger(DocumentCRUDQuickstart.class);

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
        DocumentCRUDQuickstart p = new DocumentCRUDQuickstart();

        try {
            logger.info("Starting SYNC main");
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
                .buildClient();


        createDatabaseIfNotExists();
        createContainerIfNotExists();

        createDocument();
        readDocumentById();
        //readAllDocumentsInContainer(); <Deprecated>
        queryDocuments();
        replaceDocument();
        upsertDocument();
        replaceDocumentWithConditionalEtagCheck();
        readDocumentOnlyIfChanged();
        // deleteDocument() is called at shutdown()

    }

    // Database Create
    private void createDatabaseIfNotExists() throws Exception {
        logger.info("Create database " + databaseName + " if not exists...");

        //  Create database if not exists
        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName);
        database = client.getDatabase(databaseResponse.getProperties().getId());

        logger.info("Done.");
    }

    // Container create
    private void createContainerIfNotExists() throws Exception {
        logger.info("Create container " + containerName + " if not exists.");

        //  Create container if not exists
        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(containerName, "/lastName");

        // Provision throughput
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);

        //  Create container with 200 RU/s
        CosmosContainerResponse containerResponse = database.createContainerIfNotExists(containerProperties, throughputProperties);
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
        container.createItem(family,new PartitionKey(family.getLastName()),new CosmosItemRequestOptions());

        logger.info("Done.");
    }

    // Document read
    private void readDocumentById() throws Exception {
        logger.info("Read document " + documentId + " by ID.");

        //  Read document by ID
        Family family = container.readItem(documentId,new PartitionKey(documentLastName),Family.class).getItem();

        // Check result
        logger.info("Finished reading family " + family.getId() + " with partition key " + family.getLastName());

        logger.info("Done.");
    }

    private void queryDocuments() throws Exception {
        logger.info("Query documents in the container " + containerName + ".");

        String sql = "SELECT * FROM c WHERE c.lastName = 'Witherspoon'";

        CosmosPagedIterable<Family> filteredFamilies = container.queryItems(sql, new CosmosQueryRequestOptions(), Family.class);

        // Print
        if (filteredFamilies.iterator().hasNext()) {
            Family family = filteredFamilies.iterator().next();
            logger.info("First query result: Family with (/id, partition key) = (%s,%s)",family.getId(),family.getLastName());
        }

        logger.info("Done.");
    }

    private void replaceDocument() throws Exception {
        logger.info("Replace document " + documentId);

        // Replace existing document with new modified document
        Family family = new Family();
        family.setLastName(documentLastName);
        family.setId(documentId);
        family.setDistrict("Columbia"); // Document modification

        CosmosItemResponse<Family> famResp =
                container.replaceItem(family, family.getId(), new PartitionKey(family.getLastName()), new CosmosItemRequestOptions());

        logger.info("Request charge of replace operation: {} RU", famResp.getRequestCharge());

        logger.info("Done.");
    }

    private void upsertDocument() throws Exception {
        logger.info("Replace document " + documentId);

        // Replace existing document with new modified document (contingent on modification).
        Family family = new Family();
        family.setLastName(documentLastName);
        family.setId(documentId);
        family.setDistrict("Columbia"); // Document modification

        CosmosItemResponse<Family> famResp =
                container.upsertItem(family, new CosmosItemRequestOptions());

        logger.info("Done.");
    }

    private void replaceDocumentWithConditionalEtagCheck() throws Exception {
        logger.info("Replace document " + documentId + ", employing optimistic concurrency using ETag.");

        // Obtained current document ETag
        CosmosItemResponse<Family> famResp =
                container.readItem(documentId, new PartitionKey(documentLastName), Family.class);
        String etag = famResp.getResponseHeaders().get("etag");

        logger.info("Read document " + documentId + " to obtain current ETag: " + etag);

        // Modify document
        Family family = famResp.getItem();
        family.setRegistered(!family.isRegistered());

        // Persist the change back to the server, updating the ETag in the process
        // This models a concurrent change made to the document
        CosmosItemResponse<Family> updatedFamResp =
                container.replaceItem(family,family.getId(),new PartitionKey(family.getLastName()),new CosmosItemRequestOptions());
        logger.info("'Concurrent' update to document " + documentId + " so ETag is now " + updatedFamResp.getResponseHeaders().get("etag"));

        // Now update the document and call replace with the AccessCondition requiring that ETag has not changed.
        // This should fail because the "concurrent" document change updated the ETag.
        try {

            CosmosItemRequestOptions requestOptions = new CosmosItemRequestOptions();
            requestOptions.setIfMatchETag(etag);

            family.setDistrict("Seafood");

            CosmosItemResponse<Family> failedFamResp =
                    container.replaceItem(family,family.getId(),new PartitionKey(family.getLastName()),requestOptions);

        } catch (CosmosException cce) {
            logger.info("As expected, we have a pre-condition failure exception\n");
        }

        logger.info("Done.");
    }

    private void readDocumentOnlyIfChanged() throws Exception {
        logger.info("Read document " + documentId + " only if it has been changed, utilizing an ETag check.");

        // Read document
        CosmosItemResponse<Family> famResp =
                container.readItem(documentId, new PartitionKey(documentLastName), Family.class);
        logger.info("Read doc with status code of {}", famResp.getStatusCode());

        // Re-read but with conditional access requirement that ETag has changed.
        // This should fail.

        String etag = famResp.getResponseHeaders().get("etag");
        CosmosItemRequestOptions requestOptions = new CosmosItemRequestOptions();
        requestOptions.setIfNoneMatchETag(etag);

        CosmosItemResponse<Family> failResp =
                container.readItem(documentId, new PartitionKey(documentLastName), requestOptions, Family.class);

        logger.info("Re-read doc with status code of {} (we anticipate failure due to ETag not having changed.)", failResp.getStatusCode());

        // Replace the doc with a modified version, which will update ETag
        Family family = famResp.getItem();
        family.setRegistered(!family.isRegistered());
        CosmosItemResponse<Family> failedFamResp =
                container.replaceItem(family,family.getId(),new PartitionKey(family.getLastName()),new CosmosItemRequestOptions());
        logger.info("Modified and replaced the doc (updates ETag.)");

        // Re-read doc again, with conditional acccess requirements.
        // This should succeed since ETag has been updated.
        CosmosItemResponse<Family> succeedResp =
                container.readItem(documentId, new PartitionKey(documentLastName), requestOptions, Family.class);
        logger.info("Re-read doc with status code of {} (we anticipate success due to ETag modification.)", succeedResp.getStatusCode());

        logger.info("Done.");
    }

    // Document delete
    private void deleteADocument() throws Exception {
        logger.info("Delete document " + documentId + " by ID.");

        // Delete document
        container.deleteItem(documentId, new PartitionKey(documentLastName), new CosmosItemRequestOptions());

        logger.info("Done.");
    }

    // Database delete
    private void deleteADatabase() throws Exception {
        logger.info("Last step: delete database " + databaseName + " by ID.");

        // Delete database
        CosmosDatabaseResponse dbResp = client.getDatabase(databaseName).delete(new CosmosDatabaseRequestOptions());
        logger.info("Status code for database delete: {}",dbResp.getStatusCode());

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
