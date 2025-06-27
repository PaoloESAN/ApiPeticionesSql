package com.abd.peticionsql.model;

import java.util.List;

public class DataMartRequest {
    private String name;
    private String sourceWarehouse;
    private String sourceTable;
    private List<SelectedColumn> selectedColumns;

    public DataMartRequest() {
    }

    public DataMartRequest(String name, String sourceWarehouse, List<SelectedColumn> selectedColumns) {
        this.name = name;
        this.sourceWarehouse = sourceWarehouse;
        this.selectedColumns = selectedColumns;
    }

    public DataMartRequest(String name, String sourceWarehouse, String sourceTable,
            List<SelectedColumn> selectedColumns) {
        this.name = name;
        this.sourceWarehouse = sourceWarehouse;
        this.sourceTable = sourceTable;
        this.selectedColumns = selectedColumns;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSourceWarehouse() {
        return sourceWarehouse;
    }

    public void setSourceWarehouse(String sourceWarehouse) {
        this.sourceWarehouse = sourceWarehouse;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public List<SelectedColumn> getSelectedColumns() {
        return selectedColumns;
    }

    public void setSelectedColumns(List<SelectedColumn> selectedColumns) {
        this.selectedColumns = selectedColumns;
    }

    public static class SelectedColumn {
        private String name;
        private String type;
        private String alias;

        public SelectedColumn() {
        }

        public SelectedColumn(String name, String type, String alias) {
            this.name = name;
            this.type = type;
            this.alias = alias;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }
    }
}
