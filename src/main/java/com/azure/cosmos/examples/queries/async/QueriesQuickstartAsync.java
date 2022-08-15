// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.queries.async;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseRequestOptions;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.fasterxml.jackson.databind.JsonNode;

import reactor.core.publisher.Flux;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class QueriesQuickstartAsync {

    private CosmosAsyncClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";
    private final String documentId = UUID.randomUUID().toString();
    private final String documentLastName = "Witherspoon";

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    private static final Logger logger = LoggerFactory.getLogger(QueriesQuickstartAsync.class);

    private AtomicInteger executeQueryPrintSingleResultNumber = new AtomicInteger(0);
    private AtomicInteger executeCountQueryPrintSingleResultNumber = new AtomicInteger(0);
    private AtomicInteger executeQueryWithQuerySpecPrintSingleResultNumber = new AtomicInteger(0);

    private AtomicBoolean creatDocComplete = new AtomicBoolean(false);

    public void close() {
        client.close();
    }

    /**
     * Sample to demonstrate Azure Cosmos DB queries via Java SQL API, including queries for:
     * -All documents
     * -Equality using =
     * -Inequality using != and NOT
     * -Using range operators like >, <, >=, <=
     * -Using range operators against Strings
     * -With ORDER BY
     * -With aggregate functions
     * -With subdocuments
     * -With intra-document joins
     * -With String, math and array operators
     * -With parameterized SQL using SqlQuerySpec
     * -With explicit paging
     * -Query partitioned collections in parallel
     * -With ORDER BY for partitioned collections
     */
    public static void main(String[] args) {
        QueriesQuickstartAsync p = new QueriesQuickstartAsync();

        try {
            logger.info("Starting ASYNC main");
            p.queriesDemo();
            logger.info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(String.format("Cosmos getStarted failed with %s", e));
        } finally {
            logger.info("Closing the client");
            p.shutdown();
        }
    }

    private void queriesDemo() throws Exception {

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

        createDocument();

        // We are adding Thread.sleep to mimic the some business computation that can
        // happen while waiting for earlier processes to finish.
        Thread.sleep(1000);

        logger.info("Async doc create done.");

        //execute all the below query examples asynchronously and waiting until all done
        queryAllDocuments();
        queryWithPagingAndContinuationTokenAndPrintQueryCharge(new CosmosQueryRequestOptions());
        queryEquality();
        queryInequality();
        queryRange();
        queryRangeAgainstStrings();
        queryOrderBy();
        queryWithAggregateFunctions();
        querySubdocuments();
        queryIntraDocumentJoin();
        queryStringMathAndArrayOperators();
        queryWithQuerySpec();
        parallelQueryWithPagingAndContinuationTokenAndPrintQueryCharge();


        // We are adding Thread.sleep to mimic the some business computation that can
        // happen while waiting for earlier processes to finish.
        Thread.sleep(2000);
        logger.info("*********Finished waiting - all async queries complete.");

        // deleteDocument() is called at shutdown()

    }

    private void executeQueryPrintSingleResult(String sql) {
        logger.info("Execute query {}",sql);

        CosmosPagedFlux<Family> filteredFamilies = container.queryItems(sql, new CosmosQueryRequestOptions(), Family.class);

        // Print
        filteredFamilies.byPage(100).flatMap(filteredFamiliesResponse -> {
            for (Family family : filteredFamiliesResponse.getResults()) {
                logger.info(String.format("First query result: Family with (/id, partition key) = (%s,%s)",family.getId(),family.getLastName()));            
            }
            executeQueryPrintSingleResultNumber.incrementAndGet();
            return Flux.empty();
        }).subscribe();
        logger.info("The query: \"{}\" executed async and is done.", sql);
    }

    private void executeCountQueryPrintSingleResult(String sql) {
        CosmosPagedFlux<JsonNode> filteredFamilies1 = container.queryItems(sql, new CosmosQueryRequestOptions(), JsonNode.class);

        // Print
        filteredFamilies1.byPage(100).flatMap(filteredFamiliesResponse -> {
            for (JsonNode jsonnode : filteredFamiliesResponse.getResults()) {
                logger.info("Count: {}", jsonnode.toString());            
            }
            executeCountQueryPrintSingleResultNumber.incrementAndGet();
            return Flux.empty();
        }).subscribe();

        logger.info("The query: \"{}\" executed async and is done.", sql);
    }

    private void executeQueryWithQuerySpecPrintSingleResult(SqlQuerySpec querySpec) {
        logger.info("Execute query {}",querySpec.getQueryText());

        CosmosPagedFlux<Family> filteredFamilies = container.queryItems(querySpec, new CosmosQueryRequestOptions(), Family.class);

        // Print
        filteredFamilies.byPage(100).flatMap(filteredFamiliesResponse -> {
            for (Family family : filteredFamiliesResponse.getResults()) {
                logger.info(String.format("First query result: Family with (/id, partition key) = (%s,%s)",family.getId(),family.getLastName()));
            }
            executeQueryWithQuerySpecPrintSingleResultNumber.incrementAndGet();
            return Flux.empty();
        }).subscribe();

        logger.info("The query: \"{}\" executed async and is done.",querySpec.getQueryText());
    }

    // Database Create
    private void createDatabaseIfNotExists() throws Exception {
        logger.info("Create database {} if not exists...", databaseName);

        //  Create database if not exists
        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName).block();
        database = client.getDatabase(databaseResponse.getProperties().getId());

        logger.info("createDatabaseIfNotExists done synchronously.");
    }

    // Container create
    private void createContainerIfNotExists() throws Exception {
        logger.info("Create container {} if not exists.", containerName);

        //  Create container if not exists
        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(containerName, "/lastName");

        // Provision throughput
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);

        //  Create container with 200 RU/s
        CosmosContainerResponse containerResponse = database.createContainerIfNotExists(containerProperties, throughputProperties).block();
        container = database.getContainer(containerResponse.getProperties().getId());

        logger.info("createContainerIfNotExists done synchronously.");
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
        container.createItem(family, new PartitionKey(family.getLastName()), new CosmosItemRequestOptions())
                .doOnSuccess((response) -> {
                    logger.info("created doc with id: {}", response.getItem().getId());
                    this.creatDocComplete.set(true);
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).subscribe();
    }

    private void queryAllDocuments() throws Exception {
        logger.info("Query all documents.");
        executeQueryPrintSingleResult("SELECT * FROM c");
    }


    private void queryWithPagingAndContinuationTokenAndPrintQueryCharge(CosmosQueryRequestOptions options) throws Exception {
        logger.info("Query with paging and continuation token; print the total RU charge of the query");

        String query = "SELECT * FROM Families";

        int pageSize = 100; //No of docs per page
        int currentPageNumber = 1;
        int documentNumber = 0;
        String continuationToken = null;

        double requestCharge = 0.0;

        // First iteration (continuationToken = null): Receive a batch of query response pages
        // Subsequent iterations (continuationToken != null): Receive subsequent batch of query response pages, with continuationToken indicating where the previous iteration left off
        do {
            logger.info("Receiving a set of query response pages.");
            logger.info("Continuation Token: {}\n", continuationToken);

            CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();

            Iterable<FeedResponse<Family>> feedResponseIterator =
                    container.queryItems(query, queryOptions, Family.class).byPage(continuationToken,pageSize).toIterable();

            for (FeedResponse<Family> page : feedResponseIterator) {
                logger.info(String.format("Current page number: %d", currentPageNumber));
                 // Access all of the documents in this result page
                for (Family docProps : page.getResults()) {
                    documentNumber++;
                }

                // Accumulate the request charge of this page
                requestCharge += page.getRequestCharge();

                // Page count so far
                logger.info(String.format("Total documents received so far: %d", documentNumber));

                // Request charge so far
                logger.info(String.format("Total request charge so far: %f\n", requestCharge));

                // Along with page results, get a continuation token
                // which enables the client to "pick up where it left off"
                // in accessing query response pages.
                continuationToken = page.getContinuationToken();

                currentPageNumber++;
            }

        } while (continuationToken != null);

        logger.info(String.format("Total request charge: %f\n", requestCharge));
    }

    private void parallelQueryWithPagingAndContinuationTokenAndPrintQueryCharge() throws Exception {
        logger.info("Parallel implementation of:");

        CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();

        // 0 maximum parallel tasks, effectively serial execution
        options.setMaxDegreeOfParallelism(0);
        options.setMaxBufferedItemCount(100);
        queryWithPagingAndContinuationTokenAndPrintQueryCharge(options);

        // 1 maximum parallel tasks, 1 dedicated asynchronous task to continuously make REST calls
        options.setMaxDegreeOfParallelism(1);
        options.setMaxBufferedItemCount(100);
        queryWithPagingAndContinuationTokenAndPrintQueryCharge(options);

        // 10 maximum parallel tasks, a maximum of 10 dedicated asynchronous tasks to continuously make REST calls
        options.setMaxDegreeOfParallelism(10);
        options.setMaxBufferedItemCount(100);
        queryWithPagingAndContinuationTokenAndPrintQueryCharge(options);

        logger.info("Done with parallel queries.");
    }

    private void queryEquality() throws Exception {
        logger.info("Query for equality using =");

        executeQueryPrintSingleResult("SELECT * FROM c WHERE c.id = '" + documentId + "'");
    }

    private void queryInequality() throws Exception {
        logger.info("Query for inequality");

        executeQueryPrintSingleResult("SELECT * FROM c WHERE c.id != '" + documentId + "'");
        executeQueryPrintSingleResult("SELECT * FROM c WHERE c.id <> '" + documentId + "'");

        // Combine equality and inequality
        executeQueryPrintSingleResult("SELECT * FROM c WHERE c.lastName = '" + documentLastName + "' AND c.id != '" + documentId + "'");
    }

    private void queryRange() throws Exception {
        logger.info("Numerical range query");

        // Numerical range query
        executeQueryPrintSingleResult("SELECT * FROM Families f WHERE f.Children[0].Grade > 5");
    }

    private void queryRangeAgainstStrings() throws Exception {
        logger.info("String range query");

        // String range query
        executeQueryPrintSingleResult("SELECT * FROM Families f WHERE f.Address.State > 'NY'");
    }

    private void queryOrderBy() throws Exception {
        logger.info("ORDER BY queries");

        // Numerical ORDER BY
        executeQueryPrintSingleResult("SELECT * FROM Families f WHERE f.LastName = 'Andersen' ORDER BY f.Children[0].Grade");
    }

    private void queryDistinct() throws Exception {
        logger.info("DISTINCT queries");

        // DISTINCT query
        executeQueryPrintSingleResult("SELECT DISTINCT c.lastName from c");
    }

    private void queryWithAggregateFunctions() throws Exception {
        logger.info("Aggregate function queries");

        // Basic query with aggregate functions
        executeCountQueryPrintSingleResult("SELECT VALUE COUNT(f) FROM Families f WHERE f.LastName = 'Andersen'");

        // Query with aggregate functions within documents
        executeCountQueryPrintSingleResult("SELECT VALUE COUNT(child) FROM child IN f.Children");
    }

    private void querySubdocuments() throws Exception {
        // Cosmos DB supports the selection of sub-documents on the server, there
        // is no need to send down the full family record if all you want to display
        // is a single child

        logger.info("Subdocument query");

        executeQueryPrintSingleResult("SELECT VALUE c FROM c IN f.Children");
    }

    private void queryIntraDocumentJoin() throws Exception {
        // Cosmos DB supports the notion of an Intra-document Join, or a self-join
        // which will effectively flatten the hierarchy of a document, just like doing
        // a self JOIN on a SQL table

        logger.info("Intra-document joins");

        // Single join
        executeQueryPrintSingleResult("SELECT f.id FROM Families f JOIN c IN f.Children");

        // Two joins
        executeQueryPrintSingleResult("SELECT f.id as family, c.FirstName AS child, p.GivenName AS pet " +
                                           "FROM Families f " +
                                           "JOIN c IN f.Children " +
                                           "join p IN c.Pets");

        // Two joins and a filter
        executeQueryPrintSingleResult("SELECT f.id as family, c.FirstName AS child, p.GivenName AS pet " +
                                           "FROM Families f " +
                                           "JOIN c IN f.Children " +
                                           "join p IN c.Pets " +
                                           "WHERE p.GivenName = 'Fluffy'");
    }

    private void queryStringMathAndArrayOperators() throws Exception {
        logger.info("Queries with string, math and array operators");

        // String STARTSWITH operator
        executeQueryPrintSingleResult("SELECT * FROM family WHERE STARTSWITH(family.LastName, 'An')");

        // Round down numbers with FLOOR
        executeQueryPrintSingleResult("SELECT VALUE FLOOR(family.Children[0].Grade) FROM family");

        // Get number of children using array length
        executeQueryPrintSingleResult("SELECT VALUE ARRAY_LENGTH(family.Children) FROM family");
    }

    private void queryWithQuerySpec() throws Exception {
        logger.info("Query with SqlQuerySpec");

        CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
        options.setPartitionKey(new PartitionKey("Witherspoon"));

        // Simple query with a single property equality comparison
        // in SQL with SQL parameterization instead of inlining the
        // parameter values in the query string

        ArrayList<SqlParameter> paramList = new ArrayList<SqlParameter>();
        paramList.add(new SqlParameter("@id", "AndersenFamily"));
        SqlQuerySpec querySpec = new SqlQuerySpec(
                "SELECT * FROM Families f WHERE (f.id = @id)",
                paramList);

        executeQueryWithQuerySpecPrintSingleResult(querySpec);

        // Query using two properties within each document. WHERE Id = "" AND Address.City = ""
        // notice here how we are doing an equality comparison on the string value of City

        paramList = new ArrayList<SqlParameter>();
        paramList.add(new SqlParameter("@id", "AndersenFamily"));
        paramList.add(new SqlParameter("@city", "Seattle"));
        querySpec = new SqlQuerySpec(
                "SELECT * FROM Families f WHERE f.id = @id AND f.Address.City = @city",
                paramList);

        executeQueryWithQuerySpecPrintSingleResult(querySpec);
    }

    // Document delete
    private void deleteADocument() throws Exception {
        logger.info("Delete document {} by ID.", documentId);

        // Delete document
        container.deleteItem(documentId, new PartitionKey(documentLastName), new CosmosItemRequestOptions()).block();

        logger.info("Done.");
    }

    // Database delete
    private void deleteADatabase() throws Exception {
        logger.info("Last step: delete database {} by ID.", databaseName);

        // Delete database
        CosmosDatabaseResponse dbResp = client.getDatabase(databaseName).delete(new CosmosDatabaseRequestOptions()).block();
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
