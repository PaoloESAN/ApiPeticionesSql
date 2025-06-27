package com.abd.peticionsql.model;

public class WarehouseQueryRequest {
    private String database;
    private String query;

    public WarehouseQueryRequest() {
    }

    public WarehouseQueryRequest(String database, String query) {
        this.database = database;
        this.query = query;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getWarehouse() {
        return database;
    }

    public void setWarehouse(String warehouse) {
        this.database = warehouse;
    }
}
