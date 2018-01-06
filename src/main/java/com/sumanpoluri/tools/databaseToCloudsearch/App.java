package com.sumanpoluri.tools.databaseToCloudsearch;

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

    //==================================================================================================================
    // Main method
    //==================================================================================================================
    public static void main( String[] args )
    {

        System.out.println( "Started..." );
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
        System.out.println( "...ended in " + ((endTime-startTime)/1000) + "s.");
    }

}