package com.abd.peticionsql.model;

import java.util.List;

public class WarehouseListInfo {
    private String name;
    private List<String> tables;

    public WarehouseListInfo() {
    }

    public WarehouseListInfo(String name, List<String> tables) {
        this.name = name;
        this.tables = tables;
    }

    // Getters y Setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }
}
