package com.sumanpoluri.tools.databaseToCloudsearch;

import java.text.DateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Main class of the program
 */
public class App
{
    //==================================================================================================================
    // Class fields
    //==================================================================================================================
    private static final String DB_HOST = System.getProperty("DB_HOST");
    private static final String DB_PORT = System.getProperty("DB_PORT");
    private static final String DB_USER = System.getProperty("DB_USER");
    private static final String DB_PASSWORD = System.getProperty("DB_PASSWORD");
    private static final String DB_NAME = System.getProperty("DB_NAME");
    private static final DateFormat DF_FULL = DateFormat.getDateTimeInstance(
            DateFormat.FULL,
            DateFormat.FULL,
            Locale.US);

    //==================================================================================================================
    // Main method
    //==================================================================================================================
    public static void main( String[] args )
    {

        System.out.println(
                DF_FULL.format(new Date()) +
                        ": Started...");
        long startTime = System.currentTimeMillis();
        ExtractAndUpload extractAndUpload = new ExtractAndUpload(
                DB_USER,
                DB_PASSWORD,
                DB_HOST,
                DB_PORT,
                DB_NAME
        );
        extractAndUpload.run();
        long endTime = System.currentTimeMillis();
        System.out.println(
                DF_FULL.format(new Date()) +
                        ": ...ended in " + ((endTime-startTime)/1000) + "s.");
    }

}