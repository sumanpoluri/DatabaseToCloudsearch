package com.sumanpoluri.tools.databaseToCloudsearch;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.cloudsearchdomain.AmazonCloudSearchDomain;
import com.amazonaws.services.cloudsearchdomain.AmazonCloudSearchDomainClient;
import com.amazonaws.services.cloudsearchdomain.model.ContentType;
import com.amazonaws.services.cloudsearchdomain.model.DocumentServiceException;
import com.amazonaws.services.cloudsearchdomain.model.UploadDocumentsRequest;
import com.amazonaws.services.cloudsearchdomain.model.UploadDocumentsResult;
import org.json.JSONArray;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Uploads the documents to AWS Cloudsearch.
 */
public class Uploader extends BaseUploader {
    //==================================================================================================================
    // Class fields
    //==================================================================================================================
    private static final String ACCESS_KEY = System.getProperty("AWS_ACCESS_KEY_ID");
    private static final String SECRET_KEY = System.getProperty("AWS_SECRET_ACCESS_KEY");
    private static final String CS_DOC_ENDPOINT = System.getProperty("AWS_CS_DOC_ENDPOINT");
    private static final String SIGNING_REGION = System.getProperty("AWS_SIGNING_REGION");
    private static final DecimalFormat DF_2_DECIMALS = new DecimalFormat("#0.00");
    private static final DateFormat DF_FULL = DateFormat.getDateTimeInstance(
            DateFormat.FULL,
            DateFormat.FULL,
            Locale.US);
    /**
     * Document batches are limited to one batch every 10 seconds
     */
    private static final long INTERVAL_BETWEEN_BATCHES = 10000; // in milliseconds
    private static AmazonCloudSearchDomain domain;
    private static long lastUploadedTime = 0L;

    //==================================================================================================================
    // Constructors
    //==================================================================================================================
    public Uploader() { }

    //==================================================================================================================
    // Methods
    //==================================================================================================================

    /**
     * Uploads the batch to AWS Cloudsearch.
     *
     * @param batch Batch of documents with the data.
     */
    public static void uploadBatch(JSONArray batch) {
        InputStream inputStream = new ByteArrayInputStream(batch.toString().getBytes(StandardCharsets.UTF_8));
        UploadDocumentsRequest req = new UploadDocumentsRequest()
                .withDocuments(inputStream)
                .withContentLength(Long.valueOf(Utils.getBatchSize(batch)))
                .withContentType(ContentType.Applicationjson);
        // Per AWS CloudSearch developer docs:
        // Document batches are limited to one batch every 10 seconds and 5 MB per batch.
        // So, wait for 10s before submitting another batch
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastUploadedTime < INTERVAL_BETWEEN_BATCHES) {
            try {
                Thread.sleep(10000 - (currentTime - lastUploadedTime));
            } catch (InterruptedException e) {
                System.err.println("Thread sleep failed");
                e.printStackTrace();
            }
        }
        lastUploadedTime = currentTime;
        try {
            UploadDocumentsResult result = getDomain().uploadDocuments(req);
            System.out.println(
                    DF_FULL.format(new Date()) +
                            ": Submitted batch upload - size = " +
                            DF_2_DECIMALS.format(Utils.getBatchSize(batch) / (double) (1024 * 1024)) + " MB, " +
                            "# of documents = " + Utils.getNumberOfDocsInBatch(batch) + " documents...");
            handleResult(result, lastUploadedTime);
        } catch (DocumentServiceException e) {
            try {
                writeToFile(req.getDocuments());
            } catch (IOException e1) {
                System.err.println("Failed to log the data that caused the DocumentServiceException");
                e1.printStackTrace();
            }

            throw e;
        }
    }

    /**
     * Shuts down the domain client.
     */
    public static void done() {
        domain.shutdown();
    }

    /**
     * Returns the Cloudsearch domain client. Builds one if none already available.
     *
     * @return A AmazonCloudSearchDomain object
     */
    private static AmazonCloudSearchDomain getDomain() {
        if (domain != null) {
            return domain;
        }

        AWSCredentials awsCredentials = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);
        domain = AmazonCloudSearchDomainClient
                .builder()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(CS_DOC_ENDPOINT, SIGNING_REGION))
                .build();
        return domain;
    }

}
