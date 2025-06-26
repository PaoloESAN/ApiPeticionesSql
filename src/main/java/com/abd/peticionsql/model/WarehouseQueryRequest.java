package com.abd.peticionsql.model;

public class WarehouseQueryRequest {
    private String warehouse;
    private String query;

    public WarehouseQueryRequest() {
    }

    public WarehouseQueryRequest(String warehouse, String query) {
        this.warehouse = warehouse;
        this.query = query;
    }

    // Getters y Setters
    public String getWarehouse() {
        return warehouse;
    }

    public void setWarehouse(String warehouse) {
        this.warehouse = warehouse;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
