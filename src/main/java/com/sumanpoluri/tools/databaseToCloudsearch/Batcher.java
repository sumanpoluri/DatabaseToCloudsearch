package com.sumanpoluri.tools.databaseToCloudsearch;

import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;

/**
 * Batches the documents and invokes the uploader when ready.
 */
public class Batcher {
    //==================================================================================================================
    // Class fields
    //==================================================================================================================
    /**
     * Maximum 5 MB per batch allowed
     */
    private static final Integer MAX_BATCH_SIZE = 5000000; // in bytes
    private static final DecimalFormat DF_2_DECIMALS = new DecimalFormat("#0.00");

    //==================================================================================================================
    // Instance fields
    //==================================================================================================================
    private Integer batchesUploaded = 0;
    private Integer documentsUploaded = 0;
    private JSONArray batch;
    private JSONArray batchOldVersion;

    //==================================================================================================================
    // Constructors
    //==================================================================================================================
    /**
     * Main constructor
     */
    public Batcher() {
        this.batch = new JSONArray();
        this.batchOldVersion = new JSONArray();
    }

    //==================================================================================================================
    // Methods
    //==================================================================================================================
    /**
     * Returns the number of batches uploaded in this run
     *
     * @return An Integer
     */
    public Integer getBatchesUploaded() {
        return batchesUploaded;
    }

    /**
     * Returns the number of documents uploaded in this run
     *
     * @return An Integer
     */
    public Integer getDocumentsUploaded() {
        return documentsUploaded;
    }

    /**
     * Adds a document to the batch. If the batch size reaches the max allowed, the uploader is invoked.
     *
     * @param id Document id
     * @param fields A JSON Object containing the fields and data to build the document
     */
    public void addDocument(
            String id,
            JSONObject fields) {
        // Final document. This is to ensure the last document is not missed.
        if (id == null && getBatchSize(this.batch) > 0) {
            // Upload batch
            uploadBatch(
                    this.batch,
                    true);
            return;
        }

        // Build the document
        Document document = new Document(
                "add",
                id,
                fields);
        this.batch.put(document.toJSONObj());

        // Try to get the batch as close to the max allowed size as possible.
        // With a factor of 1.0, there is a risk of going beyond the max allowed size by a few bytes. Tweak this as
        // needed.
        if (getBatchSize(this.batch) > (MAX_BATCH_SIZE * 0.995)) {
            if (getBatchSize(this.batch) > MAX_BATCH_SIZE) {
                if (getBatchSize(this.batchOldVersion) > MAX_BATCH_SIZE) {
                    // Batch is larger than max allowed size even without the last document.
                    // So, sending the batch by excluding the latest document will not help in this case.
                    // No way to send a partial batch. So error.
                    throw new RuntimeException("Batch size exceeded max allowed size");
                } else {
                    // Batch exceeds the max allowed size with the latest document.
                    // So, upload batch without the latest document.
                    uploadBatch(
                            this.batchOldVersion,
                            false);

                    // Clear batch
                    this.batch = new JSONArray();

                    // Add latest document to batch
                    this.batch.put(document.toJSONObj());
                }
            } else {
                // Upload batch
                uploadBatch(
                        this.batch,
                        false);

                // Clear batch
                this.batch = new JSONArray();
            }

        }

        // Keep a copy of the batch
        this.batchOldVersion = new JSONArray(this.batch.toString());
    }

    /**
     * Invokes the batch uploader.
     *
     * @param obj The JSONArray object representing the batch to upload
     * @param finalCall Is this the final batch to upload
     */
    private void uploadBatch(
            JSONArray obj,
            Boolean finalCall) {
        Uploader.uploadBatch(obj);
        if (finalCall) {
            Uploader.done();
        }

        batchesUploaded++;
        documentsUploaded += getNumberOfDocsInBatch(obj);
        System.out.println("Uploaded batch - size " + DF_2_DECIMALS.format(getBatchSize(obj) / (double) (1024 * 1024)) + " MB, # of documents = " + getNumberOfDocsInBatch(obj) + " documents...");
    }

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
