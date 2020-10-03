// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.storedprocedure.sync;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.CustomPOJO;
import com.azure.cosmos.implementation.Utils;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosStoredProcedureProperties;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SampleStoredProcedure {

    private CosmosClient client;

    private final String databaseName = "SprocTestDB";
    private final String containerName = "SprocTestContainer";

    private CosmosDatabase database;
    private CosmosContainer container;

    private String sprocId;

    protected static Logger logger = LoggerFactory.getLogger(SampleStoredProcedure.class);
    private static final ObjectMapper OBJECT_MAPPER = Utils.getSimpleObjectMapper();

    public void close() {
        client.close();
    }

    /**
     * Stored Procedure Example
     * <p>
     * This sample code demonstrates creation, execution, and effects of stored procedures
     * using Java SDK. A stored procedure is created which will insert a JSON object into
     * a Cosmos DB container. The sample executes the stored procedure and then performs
     * a point-read to confirm that the stored procedure had the intended effect.
     */
    //  <Main>
    public static void main(String[] args) {
        SampleStoredProcedure p = new SampleStoredProcedure();

        try {
            p.sprocDemo();
            logger.info("Demo complete, please hold while resources are released");
            p.shutdown();
            logger.info("Done.\n");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(String.format("Cosmos getStarted failed with %s", e));
            p.close();
        } finally {
        }
    }

    //  </Main>

    private void sprocDemo() throws Exception {
        //Setup client, DB, and the container for which we will create stored procedures
        //The container partition key will be id
        setUp();

        //Create stored procedure and list all stored procedures that have been created.
        createStoredProcedure();
        readAllSprocs();

        //Execute the stored procedure, which we expect will create an item with id test_doc
        executeStoredProcedure();

        //Create a stored procedure which takes an array argument and list all stored procedures that have been created
        createStoredProcedureArrayArg();
        readAllSprocs();

        //Execute the stored procedure which takes an array argument
        executeStoredProcedureArrayArg();

    }

    public void setUp() throws Exception {
        logger.info("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        ArrayList<String> preferredRegions = new ArrayList<String>();
        preferredRegions.add("West US");

        //  Create sync client
        //  <CreateSyncClient>
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .preferredRegions(preferredRegions)
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .contentResponseOnWriteEnabled(true)
                .buildClient();

        logger.info("Create database " + databaseName + " with container " + containerName + " if either does not already exist.\n");

        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName);

        database = client.getDatabase(databaseResponse.getProperties().getId());

        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(containerName, "/city");

        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);

        CosmosContainerResponse containerResponse = database.createContainerIfNotExists(containerProperties, throughputProperties);
        container = database.getContainer(containerResponse.getProperties().getId());
    }

    public void shutdown() throws Exception {
        //Safe clean & close
        deleteStoredProcedure();
    }

    public void createStoredProcedure() throws Exception {
        logger.info("Creating stored procedure...");

        sprocId = "createMyDocument";
        String sprocBody = "function createMyDocument() {\n" +
                "var documentToCreate = {\"id\":\"test_doc\", \"city\":\"Seattle\"}\n" +
                "var context = getContext();\n" +
                "var collection = context.getCollection();\n" +
                "var accepted = collection.createDocument(collection.getSelfLink(), documentToCreate,\n" +
                "    function (err, documentCreated) {\n" +
                "if (err) throw new Error('Error' + err.message);\n" +
                "context.getResponse().setBody(documentCreated.id)\n" +
                "});\n" +
                "if (!accepted) return;\n" +
                "}";
        CosmosStoredProcedureProperties storedProcedureDef = new CosmosStoredProcedureProperties(sprocId, sprocBody);
        container.getScripts()
                .createStoredProcedure(storedProcedureDef,
                        new CosmosStoredProcedureRequestOptions());
    }

    public void createStoredProcedureArrayArg() throws Exception {
        logger.info("Creating stored procedure...");

        sprocId = "createMyDocument";
        String sprocBody = "function " + sprocId + "ArrayArg(jsonArray) {\n" +
        "    var context = getContext();\n" +
        "    var container = context.getCollection();\n" +
        "    //validate if input is valid json\n" +
        "    if (typeof jsonArray === \"string\") {\n" +
        "       try {\n" +
        "            jsonArray = JSON.parse(jsonArray);\n" +
        "        } catch (e) {\n" +
        "            throw \"Bad input to store procedure should be array of json string.\";\n" +
        "        }\n" +
        "    } else {\n" +
        "        throw \"Bad input to store procedure should be array of json string.\";\n" +
        "    }\n" +
        "    var resultDocuments = [];\n" +
        "    jsonArray.forEach(function(jsonDoc) {\n" +
        "        if (jsonDoc.isUpdate != undefined && jsonDoc.isUpdate === true) {\n" +
        "            var accepted = container.replaceDocument(jsonDoc._self, jsonDoc, { etag: jsonDoc._etag },\n" +
        "            function (err, docReplaced) {\n" +
        "                if (err) throw new Error('Error' + err.message);\n" +
        "                resultDocuments.push(docReplaced);\n" +
        "            });\n" +
        "            if (!accepted) throw \"Unable to update document, abort \";\n" +
        "        } else {\n" +
        "            var accepted = container.createDocument(container.getSelfLink(), jsonDoc,\n" +
        "                    function (err, itemCreated) {\n" +
        "                if (err) throw new Error('Error' + err.message);\n" +
        "                resultDocuments.push(itemCreated);\n" +
        "            });\n" +
        "            if (!accepted) throw \"Unable to create document, abort \";\n" +
        "        }\n" +
        "    });\n" +
        "    context.getResponse().setBody(resultDocuments);\n" +
        "}\n";

        CosmosStoredProcedureProperties storedProcedureDef = new CosmosStoredProcedureProperties(sprocId + "ArrayArg", sprocBody);
        container.getScripts()
                .createStoredProcedure(storedProcedureDef,
                        new CosmosStoredProcedureRequestOptions());
    }

    private void readAllSprocs() throws Exception {
        logger.info("Listing all stored procedures associated with container " + containerName + "\n");

        CosmosPagedIterable<CosmosStoredProcedureProperties> feedResponseIterable =
                container.getScripts().readAllStoredProcedures();

        Iterator<CosmosStoredProcedureProperties> feedResponseIterator = feedResponseIterable.iterator();

        while (feedResponseIterator.hasNext()) {
            CosmosStoredProcedureProperties storedProcedureProperties = feedResponseIterator.next();
            logger.info(String.format("Stored Procedure: %s", storedProcedureProperties));
        }
    }

    public void executeStoredProcedure() throws Exception {
        logger.info(String.format("Executing stored procedure %s...\n\n", sprocId));

        CosmosStoredProcedureRequestOptions options = new CosmosStoredProcedureRequestOptions();
        options.setPartitionKey(new PartitionKey("Seattle"));
        CosmosStoredProcedureResponse executeResponse = container.getScripts()
                .getStoredProcedure(sprocId)
                .execute(null, options);

        logger.info(String.format("Stored procedure %s returned %s (HTTP %d), at cost %.3f RU.\n",
                sprocId,
                executeResponse.getResponseAsString(),
                executeResponse.getStatusCode(),
                executeResponse.getRequestCharge()));
    }

    public void executeStoredProcedureArrayArg() throws Exception {
        logger.info(String.format("Executing stored procedure %s...\n\n", sprocId+"ArrayArg"));

        String partitionValue = "Seattle";
        CosmosStoredProcedureRequestOptions options = new CosmosStoredProcedureRequestOptions();
        options.setPartitionKey(new PartitionKey(partitionValue));

        List<Object> pojos = new ArrayList<>();
        pojos.add(new CustomPOJO("idA", partitionValue));
        pojos.add(new CustomPOJO("idB", partitionValue));
        pojos.add(new CustomPOJO("idC", partitionValue));

        List<Object> sproc_args = new ArrayList<>();
        sproc_args.add(OBJECT_MAPPER.writeValueAsString(pojos));
        CosmosStoredProcedureResponse executeResponse = container.getScripts()
                .getStoredProcedure(sprocId+"ArrayArg")
                .execute(sproc_args, options);

        logger.info(String.format("Stored procedure %s returned %s (HTTP %d), at cost %.3f RU.\n",
                sprocId+"ArrayArg",
                executeResponse.getResponseAsString(),
                executeResponse.getStatusCode(),
                executeResponse.getRequestCharge()));
    }

    public void deleteStoredProcedure() throws Exception {
        logger.info("-Deleting stored procedure...\n");
        container.getScripts()
                .getStoredProcedure(sprocId)
                .delete();
        logger.info("-Deleting database...\n");
        database.delete();
        logger.info("-Closing client instance...\n");
        client.close();
        logger.info("Done.");
    }
}
