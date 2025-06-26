package com.abd.peticionsql.model;

import java.util.List;

public class DataWarehouseRequest {
    private String name;
    private String tableName;
    private List<SelectedTable> selectedTables;
    private List<Relationship> relationships;
    private List<SelectedColumn> selectedColumns;

    public DataWarehouseRequest() {
    }

    // Getters y Setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<SelectedTable> getSelectedTables() {
        return selectedTables;
    }

    public void setSelectedTables(List<SelectedTable> selectedTables) {
        this.selectedTables = selectedTables;
    }

    public List<Relationship> getRelationships() {
        return relationships;
    }

    public void setRelationships(List<Relationship> relationships) {
        this.relationships = relationships;
    }

    public List<SelectedColumn> getSelectedColumns() {
        return selectedColumns;
    }

    public void setSelectedColumns(List<SelectedColumn> selectedColumns) {
        this.selectedColumns = selectedColumns;
    }

    public static class SelectedTable {
        private String database;
        private String table;
        private String alias;

        public SelectedTable() {
        }

        // Getters y Setters
        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }
    }

    public static class Relationship {
        private TableRef table1;
        private TableRef table2;
        private String type;

        public Relationship() {
        }

        // Getters y Setters
        public TableRef getTable1() {
            return table1;
        }

        public void setTable1(TableRef table1) {
            this.table1 = table1;
        }

        public TableRef getTable2() {
            return table2;
        }

        public void setTable2(TableRef table2) {
            this.table2 = table2;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    public static class TableRef {
        private String database;
        private String table;
        private String column;

        public TableRef() {
        }

        // Getters y Setters
        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getColumn() {
            return column;
        }

        public void setColumn(String column) {
            this.column = column;
        }
    }

    public static class SelectedColumn {
        private String database;
        private String table;
        private String column;
        private String alias;
        private String type;

        public SelectedColumn() {
        }

        // Getters y Setters
        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getColumn() {
            return column;
        }

        public void setColumn(String column) {
            this.column = column;
        }

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }
}
