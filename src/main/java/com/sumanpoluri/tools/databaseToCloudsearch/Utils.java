package com.sumanpoluri.tools.databaseToCloudsearch;

import org.json.JSONArray;

import java.nio.charset.StandardCharsets;

/**
 * Common utility methods.
 */
public class Utils {
    //==================================================================================================================
    // Methods
    //==================================================================================================================
    /**
     * Returns the number of documents in a given batch.
     *
     * @param obj The JSONArray object representing the batch
     * @return An Integer representing the number of documents in the batch
     */
    public static Integer getNumberOfDocsInBatch(JSONArray obj) {
        return obj.length();
    }

    /**
     * Returns the size of a given batch.
     *
     * @param obj The JSONArray object representing the batch
     * @return An Integer representing the size of the batch
     */
    public static Integer getBatchSize(JSONArray obj) {
        return obj.toString().getBytes(StandardCharsets.UTF_8).length;
    }

}
