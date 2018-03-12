package com.sumanpoluri.tools.databaseToCloudsearch;

import com.amazonaws.services.cloudsearchdomain.model.DocumentServiceWarning;
import com.amazonaws.services.cloudsearchdomain.model.UploadDocumentsResult;

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
 * Base class to upload documents to AWS Cloudsearch.
 */
public abstract class BaseUploader {
    //==================================================================================================================
    // Class fields
    //==================================================================================================================
    private static final String CUSTOM_LOG_DIR = System.getProperty("LOG_DIR");
    private static final String DEFAULT_LOG_FILE_NAME_PREFIX = "DatabaseToCloudsearch";
    private static final DateFormat DF_FULL = DateFormat.getDateTimeInstance(
            DateFormat.FULL,
            DateFormat.FULL,
            Locale.US);

    //==================================================================================================================
    // Methods
    //==================================================================================================================

    /**
     * Handles the result of the upload.
     *
     * @param result An UploadDocumentsResult object that represents the result of the upload
     * @param lastUploadedTime Last uploaded time
     */
    public static void handleResult(
            UploadDocumentsResult result,
            long lastUploadedTime) {
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
    }

    /**
     * Creates a log file with the failed upload data.
     *
     * @param is InputStream with the upload data that failed.
     * @throws IOException
     */
    public static void writeToFile(InputStream is) throws IOException {
        Path logFilePath;
        if (CUSTOM_LOG_DIR == null || CUSTOM_LOG_DIR.trim().isEmpty()) {
            logFilePath = Paths.get(
                    System.getProperty("user.home"),
                    "DatabaseToCloudsearch",
                    "logs",
                    DEFAULT_LOG_FILE_NAME_PREFIX + "_upload_failure_" + System.currentTimeMillis() + ".json");
        } else {
            logFilePath = Paths.get(
                    CUSTOM_LOG_DIR,
                    DEFAULT_LOG_FILE_NAME_PREFIX + "_upload_failure_" + System.currentTimeMillis() + ".json");
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
