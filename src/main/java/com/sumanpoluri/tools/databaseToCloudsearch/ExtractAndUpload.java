package com.sumanpoluri.tools.databaseToCloudsearch;

import org.json.JSONObject;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Extracts the data from the database, converts it into a JSON format document and uploads the data through the Batcher
 * object.
 */
public class ExtractAndUpload {
    //==================================================================================================================
    // Class fields
    //==================================================================================================================
    /**
     * Per AWS CloudSearch developer docs:
     * Dates and times are specified in UTC (Coordinated Universal Time) according to IETF RFC3339:
     * yyyy-mm-ddTHH:mm:ss.SSSZ. In UTC, for example, 5:00 PM August 23, 1970 is: 1970-08-23T17:00:00Z. Note that you
     * can also specify fractional seconds when specifying times in UTC. For example, 1967-01-31T23:20:50.650Z.
     * <p>
     * See https://docs.aws.amazon.com/cloudsearch/latest/developerguide/configuring-index-fields.html for more details.
     * </p>
     */
    private static final String FORMAT_TIMESTAMP = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final DateFormat TIMESTAMP_FORMATTER = new SimpleDateFormat(FORMAT_TIMESTAMP);
    /**
     * Per AWS CloudSearch developer docs:
     * Both JSON and XML batches can only contain UTF-8 characters that are valid in XML. Valid characters are the
     * control characters tab (0009), carriage return (000D), and line feed (000A), and the legal characters of Unicode
     * and ISO/IEC 10646. FFFE, FFFF, and the surrogate blocks D800–DBFF and DC00–DFFF are invalid and will cause
     * errors. (For more information, see Extensible Markup Language (XML) 1.0 (Fifth Edition).) You can use the
     * following regular expression to match invalid characters so you can remove them:
     * /[^\u0009\u000a\u000d\u0020-\uD7FF\uE000-\uFFFD]/ .
     * <p>
     * See https://docs.aws.amazon.com/cloudsearch/latest/developerguide/preparing-data.html for more details.
     * </p>
     */
    private static final String REGEX_INVALID_UTF8 = "[^\\u0009\\u000a\\u000d\\u0020-\\uD7FF\\uE000-\\uFFFD]";
    // A prefix to the document ID. This is optional. Change to blank if not needed.
    private static final String DOCUMENT_ID_PREFIX = "di_";
    /**
     * The SQL to extract data to be loaded to AWS Cloudsearch. Change this to your desired SQL. For large amount of
     * data, use the optimization strategies for your database and JDBC driver.
     * <p>
     * For e.g., see the 'ResultSet' section on this page for MySQL -
     * https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-implementation-notes.html
     * </p>
     */
    private static final String SQL_SELECT_DATA =
            "SELECT " +
                    "id, " +
                    "first_name, " +
                    "last_name, " +
                    "date_of_birth, " +
                    "join_date " +
            "FROM " +
                    "employee " +
            "ORDER BY id " +
            "LIMIT 100000 ";

    //==================================================================================================================
    // Instance fields
    //==================================================================================================================
    private String user;
    private String password;
    private String host;
    private String port;
    private String database;
    private Batcher batcher;

    //==================================================================================================================
    // Constructors
    //==================================================================================================================
    /**
     * Main constructor
     *
     * @param user Username for the database
     * @param password Password for the database
     * @param host Hostname for the database
     * @param port Port for the database
     * @param database Name of the database
     */
    public ExtractAndUpload(
            String user,
            String password,
            String host,
            String port,
            String database) {
        this.user = user;
        this.password = password;
        this.host = host;
        this.port = port;
        this.database = database;
        this.batcher = new Batcher();
    }

    //==================================================================================================================
    // Methods
    //==================================================================================================================
    /**
     * Performs the extract and upload process.
     */
    public void run() {
        Connection conn = null;
        try {
            conn = getDBConnection();
            if (conn == null) {
                System.out.println("Unable to connect to the database");
                return;
            }

            // For large amount of data, use the optimization strategies for your database and JDBC driver.
            // For e.g., see the 'ResultSet' section on this page for MySQL -
            // https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-implementation-notes.html
            // This implementation does not perform any optimizations.
            conn.setReadOnly(true);
            Statement stmt = conn.createStatement();
            stmt.setFetchSize(1000);
            ResultSet rs = stmt.executeQuery(SQL_SELECT_DATA);

            // Get column names from the SQL result
            Map<String, String> colNamesMap = new HashMap<>();
            ResultSetMetaData meta = rs.getMetaData();
            for (int i=1; i<=meta.getColumnCount(); i++) {
                colNamesMap.put(meta.getColumnName(i), meta.getColumnClassName(i));
            }

            // Get data frm the SQL
            while (rs.next()) {
                JSONObject fields = new JSONObject();
                Iterator colIter = colNamesMap.entrySet().iterator();
                while (colIter.hasNext()) {
                    Map.Entry colEntry = (Map.Entry)colIter.next();
                    switch ((String)colEntry.getValue()) {
                        case "java.lang.Long":
                            fields.put(
                                    (String)colEntry.getKey(),
                                    rs.getLong((String)colEntry.getKey()));
                            break;
                        case "java.lang.String":
                            // Removes invalid characters from the string value.
                            fields.put(
                                    (String)colEntry.getKey(),
                                    removeInvalidUTF8Chars(rs.getString((String)colEntry.getKey())));
                            break;
                        case "java.lang.Integer":
                            // The name 'score' is reserved and cannot be specified as a field name for AWS Cloudsearch
                            // indexes.
                            // Any field from the SQL that has the name 'score' is replaced with 'score_' here. Change
                            // this as needed.
                            fields.put(
                                    colEntry.getKey().equals("score")?"score_":(String)colEntry.getKey(),
                                    rs.getInt((String)colEntry.getKey()));
                            break;
                        case "java.lang.Boolean":
                            fields.put(
                                    (String)colEntry.getKey(),
                                    rs.getBoolean((String)colEntry.getKey()));
                            break;
                        case "java.sql.Timestamp":
                            // Timestamps must be formatted per AWS Cloudsearch guidelines.
                            try {
                                fields.put(
                                        (String) colEntry.getKey(),
                                        TIMESTAMP_FORMATTER.format(rs.getTimestamp((String) colEntry.getKey())));
                            } catch (SQLException e) {
                                System.err.println("Timestamp error on field " + colEntry.getKey() +
                                        " for id " + rs.getLong("id"));
                                throw e;
                            }
                            break;
                        default:
                            fields.put(
                                    (String)colEntry.getKey(),
                                    rs.getString((String)colEntry.getKey()));
                            break;
                    }
                }

                // A unique ID for the document. This is required.
                String id = DOCUMENT_ID_PREFIX + rs.getInt("id");
                this.batcher.addDocument(id, fields);
                //System.out.println(fields);
            }

            // Final call. This is to ensure the last document is not missed.
            this.batcher.addDocument(null, null);

            // Summary
            System.out.println("-----------------------------");
            System.out.println("Total batches uploaded   = " + this.batcher.getBatchesUploaded());
            System.out.println("Total documents uploaded = " + this.batcher.getDocumentsUploaded());
            System.out.println("-----------------------------");

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeDBConnection(conn);
        }
    }

    /**
     * Acquire database connection.
     *
     * @return A java.sql.Connection object
     * @throws SQLException
     */
    private Connection getDBConnection() throws SQLException {
        Connection conn = null;
        Properties connProps = new Properties();
        connProps.put("user", this.user);
        connProps.put("password", this.password);

        // Build the url of the DB connection.
        // This example shows how to build a MySQL URL with some sample connection properties at the end of the url
        // string (e.g. characterEncoding, useCursorFetch, etc.). Change them as needed.
        String url = "jdbc:mysql://" +
                this.host + ":" +
                this.port + "/" +
                this.database + "?useUnicode=yes&useAffectedRows=true&characterEncoding=utf-8&useCursorFetch=true";
        conn = DriverManager.getConnection(url, connProps);
        return conn;
    }

    /**
     * Close database connection.
     *
     * @param conn A java.sql.Connection object
     */
    private void closeDBConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Replaces invalid characters in the string with a space.
     *
     * @param input Input string.
     * @return A String object with the invalid characters replaced with a space.
     */
    private String removeInvalidUTF8Chars(String input) {
        if (input == null) {
            return input;
        }

        return input.replaceAll(REGEX_INVALID_UTF8, " ");
    }

}