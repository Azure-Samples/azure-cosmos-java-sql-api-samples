package com.azure.cosmos.examples.bulk.async;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.HttpConstants;
import com.azure.cosmos.models.CosmosBulkItemResponse;
import com.azure.cosmos.models.CosmosBulkOperationResponse;
import com.azure.cosmos.models.CosmosItemOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

public class BulkWriter {
    private static Logger logger = LoggerFactory.getLogger(BulkWriter.class);
    private final Sinks.EmitFailureHandler emitFailureHandler =
            (signalType, emitResult) -> {
                if (emitResult.equals(Sinks.EmitResult.FAIL_NON_SERIALIZED)) {
                    logger.debug("emitFailureHandler - Signal: [{}], Result: [{}]", signalType.toString(), emitResult.toString());
                    return true;
                } else {
                    logger.error("emitFailureHandler - Signal: [{}], Result: [{}]", signalType.toString(), emitResult.toString());
                    return false;
                }
            };
    Sinks.Many<CosmosItemOperation> bulkInputEmitter = Sinks.many().unicast().onBackpressureBuffer();
    private CosmosAsyncContainer cosmosAsyncContainer;


    public BulkWriter(CosmosAsyncContainer cosmosAsyncContainer) {
        this.cosmosAsyncContainer = cosmosAsyncContainer;
    }

    public void scheduleWrites(CosmosItemOperation cosmosItemOperation) {
        bulkInputEmitter.emitNext(cosmosItemOperation, emitFailureHandler);
    }


    public Flux<CosmosBulkOperationResponse> execute() {
        return cosmosAsyncContainer.executeBulkOperations(bulkInputEmitter.asFlux()).publishOn(Schedulers.boundedElastic()).map(bulkOperationResponse -> {
            processBulkOperationResponse(bulkOperationResponse.getResponse(), bulkOperationResponse.getOperation(), bulkOperationResponse.getException());
            return bulkOperationResponse;
        });
    }

    private void processBulkOperationResponse(CosmosBulkItemResponse itemResponse, CosmosItemOperation itemOperation, Exception exception) {
        if (exception != null) {
            handleException(itemOperation, exception);
        } else {
            processResponseCode(itemResponse, itemOperation);
        }

    }

    private void processResponseCode(CosmosBulkItemResponse itemResponse, CosmosItemOperation itemOperation) {
        if (itemResponse.isSuccessStatusCode()) {
            logger.info("The operation for Item ID: [{}]  Item PartitionKey Value: [{}] completed successfully with a response status code: [{}]",
                    itemOperation.getId(), itemOperation.getPartitionKeyValue(), itemResponse.getStatusCode());
        } else if (shouldRetry(itemResponse.getStatusCode())) {
            logger.info("The operation for Item ID: [{}]  Item PartitionKey Value: [{}] will be retried",
                    itemOperation.getId(), itemOperation.getPartitionKeyValue());
            //re-scheduling
            scheduleWrites(itemOperation);
        } else {
            logger.info("The operation for Item ID: [{}]  Item PartitionKey Value: [{}] did not complete successfully with a response status code: [{}]",
                    itemOperation.getId(), itemOperation.getPartitionKeyValue(), itemResponse.getStatusCode());
        }
    }

    private void handleException(CosmosItemOperation itemOperation, Exception exception) {
        if (!(exception instanceof CosmosException)) {
            logger.info("The operation for Item ID: [{}]  Item PartitionKey Value: [{}] encountered an unexpected failure",
                    itemOperation.getId(), itemOperation.getPartitionKeyValue());
        } else {
            if (shouldRetry(((CosmosException) exception).getStatusCode())) {
                logger.info("The operation for Item ID: [{}]  Item PartitionKey Value: [{}] will be retried",
                        itemOperation.getId(), itemOperation.getPartitionKeyValue());
                //re-scheduling
                scheduleWrites(itemOperation);
            }
        }
    }

    private boolean shouldRetry(int statusCode) {
        return statusCode == HttpConstants.StatusCodes.REQUEST_TIMEOUT || statusCode == HttpConstants.StatusCodes.TOO_MANY_REQUESTS;

    }

}
