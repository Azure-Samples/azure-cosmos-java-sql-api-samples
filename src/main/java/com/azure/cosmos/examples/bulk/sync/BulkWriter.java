// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/*
  The BulkWriter class is an attempt to provide guidance for creating
  a higher level abstraction over the existing low level Java Bulk API
 */
package com.azure.cosmos.examples.bulk.sync;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.HttpConstants;
import com.azure.cosmos.models.CosmosBulkExecutionOptions;
import com.azure.cosmos.models.CosmosBulkItemResponse;
import com.azure.cosmos.models.CosmosBulkOperationResponse;
import com.azure.cosmos.models.CosmosItemOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Sinks;
import java.util.concurrent.Semaphore;

public class BulkWriter {
    private static final Logger logger = LoggerFactory.getLogger(BulkWriter.class);

    private final Sinks.Many<CosmosItemOperation> bulkInputEmitter = Sinks.many().unicast().onBackpressureBuffer();
    private final int cpuCount = Runtime.getRuntime().availableProcessors();

    //Max items to be buffered to avoid out of memory error
    private final Semaphore semaphore = new Semaphore(1024 * 167 / cpuCount);

    private final Sinks.EmitFailureHandler emitFailureHandler =
            (signalType, emitResult) -> {
                if (emitResult.equals(Sinks.EmitResult.FAIL_NON_SERIALIZED)) {
                    logger.debug("emitFailureHandler - Signal: [{}], Result: [{}]", signalType, emitResult);
                    return true;
                } else {
                    logger.error("emitFailureHandler - Signal: [{}], Result: [{}]", signalType, emitResult);
                    return false;
                }
            };

    private final CosmosContainer cosmosContainer;

    public BulkWriter(CosmosContainer cosmosContainer) {
        this.cosmosContainer = cosmosContainer;
    }

    public void scheduleWrites(CosmosItemOperation cosmosItemOperation) {
        while(!semaphore.tryAcquire()) {
            logger.info("Unable to acquire permit");
        }
        logger.info("Acquired permit");
        scheduleInternalWrites(cosmosItemOperation);
    }

    private void scheduleInternalWrites(CosmosItemOperation cosmosItemOperation) {
        bulkInputEmitter.emitNext(cosmosItemOperation, emitFailureHandler);
    }

    public Iterable<CosmosBulkOperationResponse<Object>> execute() {
        return this.execute(null);
    }

    public Iterable<CosmosBulkOperationResponse<Object>> execute(CosmosBulkExecutionOptions bulkOptions) {
        if (bulkOptions == null) {
            bulkOptions = new CosmosBulkExecutionOptions();
        }
        bulkInputEmitter.tryEmitComplete();
        Iterable<CosmosBulkOperationResponse<Object>> bulkOperationResponse = cosmosContainer
                .executeBulkOperations(
                        bulkInputEmitter.asFlux().toIterable(),
                        bulkOptions);
        for (CosmosBulkOperationResponse<Object> response : bulkOperationResponse) {
            processBulkOperationResponse(
                response.getResponse(),
                response.getOperation(),
                response.getException());
        }
        semaphore.release();
        return bulkOperationResponse;
    }




    private void processBulkOperationResponse(
        CosmosBulkItemResponse itemResponse,
        CosmosItemOperation itemOperation,
        Exception exception) {

        if (exception != null) {
            handleException(itemOperation, exception);
        } else {
            processResponseCode(itemResponse, itemOperation);
        }
    }

    private void processResponseCode(
        CosmosBulkItemResponse itemResponse,
        CosmosItemOperation itemOperation) {

        if (itemResponse.isSuccessStatusCode()) {
            logger.info(
                "The operation for Item ID: [{}]  Item PartitionKey Value: [{}] completed successfully " +
                    "with a response status code: [{}]",
                itemOperation.getId(),
                itemOperation.getPartitionKeyValue(),
                itemResponse.getStatusCode());
        } else if (shouldRetry(itemResponse.getStatusCode())) {
            logger.info(
                "The operation for Item ID: [{}]  Item PartitionKey Value: [{}] will be retried",
                itemOperation.getId(),
                itemOperation.getPartitionKeyValue());
            //re-scheduling
            scheduleWrites(itemOperation);
        } else {
            logger.info(
                "The operation for Item ID: [{}]  Item PartitionKey Value: [{}] did not complete successfully " +
                    "with a response status code: [{}]",
                itemOperation.getId(),
                itemOperation.getPartitionKeyValue(),
                itemResponse.getStatusCode());
        }
    }

    private void handleException(CosmosItemOperation itemOperation, Exception exception) {
        if (!(exception instanceof CosmosException)) {
            logger.info(
                "The operation for Item ID: [{}]  Item PartitionKey Value: [{}] encountered an unexpected failure",
                itemOperation.getId(),
                itemOperation.getPartitionKeyValue());
        } else {
            if (shouldRetry(((CosmosException) exception).getStatusCode())) {
                logger.info(
                    "The operation for Item ID: [{}]  Item PartitionKey Value: [{}] will be retried",
                    itemOperation.getId(),
                    itemOperation.getPartitionKeyValue());

                //re-scheduling
                scheduleWrites(itemOperation);
            }
        }
    }

    private boolean shouldRetry(int statusCode) {
        return statusCode == HttpConstants.StatusCodes.REQUEST_TIMEOUT ||
            statusCode == HttpConstants.StatusCodes.TOO_MANY_REQUESTS;
    }
}
