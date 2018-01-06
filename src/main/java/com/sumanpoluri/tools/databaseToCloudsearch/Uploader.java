package com.sumanpoluri.tools.databaseToCloudsearch;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.cloudsearchdomain.AmazonCloudSearchDomain;
import com.amazonaws.services.cloudsearchdomain.AmazonCloudSearchDomainClient;
import com.amazonaws.services.cloudsearchdomain.model.*;
import org.json.JSONArray;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Uploads the documents to AWS Cloudsearch.
 */
public class Uploader {
    //==================================================================================================================
    // Class fields
    //==================================================================================================================
    private static final String ACCESS_KEY = System.getProperty("AWS_ACCESS_KEY_ID");
    private static final String SECRET_KEY = System.getProperty("AWS_SECRET_ACCESS_KEY");
    private static final String CS_DOC_ENDPOINT = System.getProperty("AWS_CS_DOC_ENDPOINT");
    private static final String SIGNING_REGION = System.getProperty("AWS_SIGNING_REGION");
    private static final String CUSTOM_LOG_DIR = System.getProperty("LOG_DIR");
    private static final String DEFAULT_LOG_FILE_NAME_PREFIX = "DatabaseToCloudsearch";
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
                .withContentLength(Long.valueOf(Batcher.getBatchSize(batch)))
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
            if (result.getStatus().equals("error")) {
                System.err.println(
                        DF_FULL.format(new Date()) +
                                ": Upload failed! HTTP Status Code = " + result.getSdkHttpMetadata().getHttpStatusCode());
                System.err.println("Upload failed! Errors follow...");
                for (DocumentServiceWarning warning : result.getWarnings()) {
                    System.err.println(warning);
                }
            } else {
                System.out.println(
                        DF_FULL.format(new Date()) +
                                ": Upload success! HTTP Status Code = " + result.getSdkHttpMetadata().getHttpStatusCode() +
                                ", Adds = " + result.getAdds() +
                                ", Upload took " + ((System.currentTimeMillis() - lastUploadedTime) / 1000) + "s");
            }
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

    /**
     * Creates a log file with the failed upload data.
     *
     * @param is InputStream with the upload data that failed.
     * @throws IOException
     */
    public static void writeToFile(InputStream is) throws IOException {
        Path logFilePath = null;
        if (CUSTOM_LOG_DIR == null || CUSTOM_LOG_DIR.trim().isEmpty()) {
            logFilePath = Paths.get(System.getProperty("user.home"),"DatabaseToCloudsearch", "logs", DEFAULT_LOG_FILE_NAME_PREFIX + "_upload_failure_" + System.currentTimeMillis() + ".json");
        } else {
            logFilePath = Paths.get(CUSTOM_LOG_DIR, DEFAULT_LOG_FILE_NAME_PREFIX + "_upload_failure_" + System.currentTimeMillis() + ".json");
        }

        BufferedWriter bw = Files.newBufferedWriter(logFilePath, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        char[] charArray = new char[2048];
        while (br.read(charArray) != -1) {
            bw.write(charArray);
        }
        bw.close();
        br.close();
        is.close();
    }
}
