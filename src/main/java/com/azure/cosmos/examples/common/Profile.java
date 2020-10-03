// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.common;

import com.azure.cosmos.examples.changefeed.SampleChangeFeedProcessor;
import com.azure.cosmos.implementation.Utils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.UUID;

public class Profile {

    private static long tic_ns = System.nanoTime(); // For execution timing
    protected static Logger logger = LoggerFactory.getLogger(SampleChangeFeedProcessor.class);

    /* tic/toc pair - measure ms execution time between tic() and toc_ms()
       Undefined behavior if you you do not pair 'tic()' followed by 'toc_ms()'
     */
    public static void tic() {tic_ns = System.nanoTime();}
    public static double toc_ms() {return ((double)(System.nanoTime()-tic_ns))/1000000.0;};

    /* Generate ArrayList of N unique documents (assumes /pk is id) */
    public static ArrayList<JsonNode> generateDocs(int N) {
        ArrayList<JsonNode> docs = new ArrayList<JsonNode>();
        ObjectMapper mapper = Utils.getSimpleObjectMapper();

        try {
            for (int i = 1; i <= N; i++) {
                docs.add(mapper.readTree(
                        "{" +
                                "\"id\": " +
                                "\"" + UUID.randomUUID().toString() + "\"" +
                                "}"
                ));


            }
        } catch (Exception err) {
            logger.error("Failed generating documents: ", err);
        }

        return docs;
    }

    /* Placeholder for background tasks to run during resource creation */
    public static void doOtherThings() {
        // Not much to do right now :)
    }

}
