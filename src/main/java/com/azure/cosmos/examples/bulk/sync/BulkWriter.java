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

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

public class BulkWriter {
    private static final Logger logger = LoggerFactory.getLogger(BulkWriter.class);

    private final CosmosContainer cosmosContainer;
    private final int cpuCount = Runtime.getRuntime().availableProcessors();
    private final Semaphore semaphore = new Semaphore(1024 * 167 / cpuCount);
    private final Queue<CosmosItemOperation> bufferedOperations = new ConcurrentLinkedQueue<>();
    private final int maxRetries = 5;

    public BulkWriter(CosmosContainer cosmosContainer) {
        this.cosmosContainer = cosmosContainer;
    }

    public void scheduleWrites(CosmosItemOperation cosmosItemOperation) {
        while (!semaphore.tryAcquire()) {
            logger.info("Unable to acquire permit");
        }
        logger.info("Acquired permit");
        bufferedOperations.add(cosmosItemOperation);
    }

    public Iterable<CosmosBulkOperationResponse<Object>> execute() {
        return execute(null);
    }

    public Iterable<CosmosBulkOperationResponse<Object>> execute(CosmosBulkExecutionOptions bulkOptions) {
        if (bulkOptions == null) {
            bulkOptions = new CosmosBulkExecutionOptions();
        }

        List<CosmosItemOperation> currentBatch = new ArrayList<>(bufferedOperations);
        bufferedOperations.clear();
        List<CosmosBulkOperationResponse<Object>> finalResponses = new ArrayList<>();
        int attempt = 0;

        while (!currentBatch.isEmpty() && attempt <= maxRetries) {
            logger.info("Executing bulk attempt {} with {} items", attempt + 1, currentBatch.size());

            Iterable<CosmosBulkOperationResponse<Object>> responses =
                    cosmosContainer.executeBulkOperations(currentBatch, bulkOptions);

            List<CosmosItemOperation> toRetry = new ArrayList<>();

            for (CosmosBulkOperationResponse<Object> response : responses) {
                processBulkOperationResponse(
                        response.getResponse(),
                        response.getOperation(),
                        response.getException(),
                        toRetry
                );
                finalResponses.add(response);
            }

            currentBatch = toRetry;
            attempt++;
        }

        if (!currentBatch.isEmpty()) {
            logger.error("BulkWriter: {} items failed after {} retries", currentBatch.size(), maxRetries);
        }

        semaphore.release();
        return finalResponses;
    }

    private void processBulkOperationResponse(
            CosmosBulkItemResponse itemResponse,
            CosmosItemOperation itemOperation,
            Exception exception,
            List<CosmosItemOperation> retryList) {

        if (exception != null) {
            handleException(itemOperation, exception, retryList);
        } else {
            processResponseCode(itemResponse, itemOperation, retryList);
        }
    }

    private void processResponseCode(
            CosmosBulkItemResponse itemResponse,
            CosmosItemOperation itemOperation,
            List<CosmosItemOperation> retryList) {

        if (itemResponse.isSuccessStatusCode()) {
            logger.info(
                    "Item ID [{}] with PartitionKey [{}] succeeded with status [{}]",
                    itemOperation.getId(),
                    itemOperation.getPartitionKeyValue(),
                    itemResponse.getStatusCode());
        } else if (shouldRetry(itemResponse.getStatusCode())) {
            logger.info(
                    "Item ID [{}] with PartitionKey [{}] will be retried (status [{}])",
                    itemOperation.getId(),
                    itemOperation.getPartitionKeyValue(),
                    itemResponse.getStatusCode());
            if (itemResponse.getRetryAfterDuration() != null) {
                logger.info(
                        "Item ID [{}] with PartitionKey [{}] will be retried after [{}] milliseconds",
                        itemOperation.getId(),
                        itemOperation.getPartitionKeyValue(),
                        itemResponse.getRetryAfterDuration().toMillis());
                try {
                    Thread.sleep(itemResponse.getRetryAfterDuration().toMillis());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            retryList.add(itemOperation);
        } else {
            logger.info(
                    "Item ID [{}] with PartitionKey [{}] failed with non-retryable status [{}]",
                    itemOperation.getId(),
                    itemOperation.getPartitionKeyValue(),
                    itemResponse.getStatusCode());
        }
    }

    private void handleException(
            CosmosItemOperation itemOperation,
            Exception exception,
            List<CosmosItemOperation> retryList) {

        if (!(exception instanceof CosmosException)) {
            logger.info(
                    "Item ID [{}] with PartitionKey [{}] encountered unexpected failure",
                    itemOperation.getId(),
                    itemOperation.getPartitionKeyValue());
        } else {
            int statusCode = ((CosmosException) exception).getStatusCode();
            if (shouldRetry(statusCode)) {
                logger.info(
                        "Item ID [{}] with PartitionKey [{}] will be retried due to exception status [{}]",
                        itemOperation.getId(),
                        itemOperation.getPartitionKeyValue(),
                        statusCode);
                retryList.add(itemOperation);
            } else {
                logger.error(
                        "Item ID [{}] with PartitionKey [{}] failed with non-retryable exception: {}",
                        itemOperation.getId(),
                        itemOperation.getPartitionKeyValue(),
                        exception.getMessage());
            }
        }
    }

    private boolean shouldRetry(int statusCode) {
        return statusCode == 408 ||
                statusCode == 429 ||
                statusCode == 503 ||
                statusCode == 500 ||
                statusCode == 449 ||
                statusCode == 410;
    }
}
