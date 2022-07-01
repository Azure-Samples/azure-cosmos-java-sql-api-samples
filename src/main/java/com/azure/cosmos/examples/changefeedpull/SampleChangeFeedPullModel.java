package com.azure.cosmos.examples.changefeedpull;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.examples.changefeed.SampleConfigurations;
import com.azure.cosmos.models.CosmosChangeFeedRequestOptions;
import com.azure.cosmos.models.FeedRange;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SampleChangeFeedPullModel {

    public static CosmosAsyncClient clientAsync;
    private CosmosAsyncContainer container;
    private CosmosAsyncDatabase database;

    public static final String DATABASE_NAME = "db";
    public static final String COLLECTION_NAME = "ChangeFeedPull";
    public static final String PARTITION_KEY_FIELD_NAME = "pk";
    protected static Logger logger = LoggerFactory.getLogger(SampleChangeFeedPullModel.class);

    public static void main(String[] args) {
        SampleChangeFeedPullModel p = new SampleChangeFeedPullModel();

        try {
            logger.info("Starting ASYNC main");
            p.ChangeFeedPullDemo();
            logger.info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(String.format("Cosmos getStarted failed with %s", e));
        } finally {
            logger.info("Closing the client");
            p.shutdown(p.container, p.database);
        }
    }

    public void ChangeFeedPullDemo() {

        clientAsync = this.getCosmosAsyncClient();
        Resources resources = new Resources(PARTITION_KEY_FIELD_NAME, clientAsync, DATABASE_NAME, COLLECTION_NAME);
        this.container = resources.container;
        this.database = resources.database;

        resources.insertDocuments(10, 20);

        logger.info("*************************************************************");
        logger.info("*************************************************************");
        logger.info("Reading change feed with all feed ranges on this machine...");
        logger.info("*************************************************************");
        logger.info("*************************************************************");

        // <AllFeedRanges>
        CosmosChangeFeedRequestOptions options = CosmosChangeFeedRequestOptions
                .createForProcessingFromBeginning(FeedRange.forFullRange());
        Map<Integer, String> continuations = new HashMap<>();
        int i = 0;
        while (true) {
            List<JsonNode> results;
            final Integer index = i;
            results = container
                    .queryChangeFeed(options, JsonNode.class)
                    .handle((r) -> continuations.put(index, r.getContinuationToken()))
                    .collectList()
                    .block();
            logger.info("Got " + results.size() + " items(s)");
            options = CosmosChangeFeedRequestOptions
                    .createForProcessingFromContinuation(continuations.get(i));
            i++;
            if (i >= 5) {
                // artificially breaking out of loop
                System.out.println("breaking....");
                break;
            }
        }
        // </AllFeedRanges>

        logger.info("*************************************************************");
        logger.info("*************************************************************");
        logger.info("Finished Reading change feed using all feed ranges!");
        logger.info("*************************************************************");
        logger.info("*************************************************************");

        // <GetFeedRanges>
        Mono<List<FeedRange>> feedranges = resources.container.getFeedRanges();
        List<FeedRange> feedRangeList = feedranges.block();
        // </GetFeedRanges>

        logger.info("*************************************************************");
        logger.info("*************************************************************");
        logger.info("Simulate processing change feed on two separate machines");
        logger.info("*************************************************************");
        logger.info("*************************************************************");
        logger.info("*************************************************************");
        logger.info("*************************************************************");
        logger.info("Start reading from machine 1....");
        logger.info("*************************************************************");
        logger.info("*************************************************************");

        // <Machine1>
        FeedRange range1 = feedRangeList.get(0);
        options = CosmosChangeFeedRequestOptions
                .createForProcessingFromBeginning(range1);

        Map<Integer, String> machine1continuations = new HashMap<>();

        int machine1index = 0;
        while (true) {
            List<JsonNode> results;
            final Integer index = machine1index;
            results = container
                    .queryChangeFeed(options, JsonNode.class)
                    .handle((r) -> machine1continuations.put(index, r.getContinuationToken()))
                    .collectList()
                    .block();
            logger.info("Got " + results.size() + " items(s) retrieved");
            options = CosmosChangeFeedRequestOptions
                    .createForProcessingFromContinuation(machine1continuations.get(machine1index));
            machine1index++;

            if (machine1index >= 5) {
                // artificially breaking out of loop
                System.out.println("breaking....");
                break;
            }
        }
        // </Machine1>

        logger.info("*************************************************************");
        logger.info("*************************************************************");
        logger.info("Finished reading feed ranges on machine 1!");
        logger.info("*************************************************************");
        logger.info("*************************************************************");

        logger.info("*************************************************************");
        logger.info("*************************************************************");

        logger.info("*************************************************************");
        logger.info("*************************************************************");
        logger.info("Start reading from machine 2....");
        logger.info("*************************************************************");
        logger.info("*************************************************************");

        // <Machine2>
        FeedRange range2 = feedRangeList.get(1);
        options = CosmosChangeFeedRequestOptions
                .createForProcessingFromBeginning(range2);
        Map<Integer, String> machine2continuations = new HashMap<>();
        int machine2index = 0;
        while (true) {
            List<JsonNode> results;
            final Integer index = machine2index;
            results = container
                    .queryChangeFeed(options, JsonNode.class)
                    .handle((r) -> machine2continuations.put(index, r.getContinuationToken()))
                    .collectList()
                    .block();
            logger.info("Got " + results.size() + " items(s)");
            options = CosmosChangeFeedRequestOptions
                    .createForProcessingFromContinuation(machine2continuations.get(machine2index));
            machine2index++;
            if (machine2index >= 5) {
                // artificially breaking out of loop
                System.out.println("breaking....");
                break;
            }
        }
        // </Machine2>

        logger.info("*************************************************************");
        logger.info("*************************************************************");
        logger.info("Finished reading feed ranges on machine 2!");
        logger.info("*************************************************************");
        logger.info("*************************************************************");

    }

    public CosmosAsyncClient getCosmosAsyncClient() {

        return new CosmosClientBuilder()
                .endpoint(SampleConfigurations.HOST)
                .key(SampleConfigurations.MASTER_KEY)
                .contentResponseOnWriteEnabled(true)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .buildAsyncClient();
    }

    public void close() {
        clientAsync.close();
    }

    private void shutdown(CosmosAsyncContainer container, CosmosAsyncDatabase database) {
        try {
            // To allow for the sequence to complete after subscribe() calls
            Thread.sleep(5000);
            // Clean shutdown
            logger.info("Deleting Cosmos DB resources");
            logger.info("-Deleting container...");
            if (container != null)
                container.delete().subscribe();
            logger.info("-Deleting database...");
            if (database != null)
                database.delete().subscribe();
            logger.info("-Closing the client...");
        } catch (InterruptedException err) {
            err.printStackTrace();
        } catch (Exception err) {
            logger.error("Deleting Cosmos DB resources failed, will still attempt to close the client. See stack "
                    + "trace below.");
            err.printStackTrace();
        }
        clientAsync.close();
        logger.info("Done.");
    }

}
