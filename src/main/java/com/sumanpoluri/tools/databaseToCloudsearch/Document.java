package com.sumanpoluri.tools.databaseToCloudsearch;

import org.json.JSONObject;

/**
 * Represents a document
 */
public class Document {
    //==================================================================================================================
    // Instance fields
    //==================================================================================================================
    private String type;
    private String id;
    private JSONObject fields;

    //==================================================================================================================
    // Constructors
    //==================================================================================================================
    /**
     * Main constructor
     *
     * @param type Type of document ('add','delete','update')
     * @param id ID of the document
     * @param fields A JSONObject representing the fields and data related to the document
     */
    public Document(String type, String id, JSONObject fields) {
        this.type = type;
        this.id = id;
        this.fields = fields;
    }

    //==================================================================================================================
    // Methods
    //==================================================================================================================
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public JSONObject getFields() {
        return fields;
    }

    public void setFields(JSONObject fields) {
        this.fields = fields;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Document{");
        sb.append("type='").append(type).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", fields=").append(fields);
        sb.append('}');
        return sb.toString();
    }

    /**
     * Returns the document in the form of a JSONObject.
     *
     * @return A JSONObject representation of the document.
     */
    public JSONObject toJSONObj() {
        JSONObject document = new JSONObject();
        document.put("type", this.type);
        document.put("id", this.id);
        document.put("fields", this.fields);
        return document;
    }
}
