package com.abd.peticionsql.model;

public class WarehouseInfo {
    private String name;
    private String created_date;
    private int table_count;
    private String status;

    public WarehouseInfo() {
    }

    public WarehouseInfo(String name, String created_date, int table_count, String status) {
        this.name = name;
        this.created_date = created_date;
        this.table_count = table_count;
        this.status = status;
    }

    // Getters y Setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCreated_date() {
        return created_date;
    }

    public void setCreated_date(String created_date) {
        this.created_date = created_date;
    }

    public int getTable_count() {
        return table_count;
    }

    public void setTable_count(int table_count) {
        this.table_count = table_count;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
