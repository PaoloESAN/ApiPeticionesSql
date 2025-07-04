package com.abd.peticionsql.services;

import com.abd.peticionsql.model.ColumnaInfo;
import com.abd.peticionsql.model.DatabaseInfo;
import com.abd.peticionsql.model.DataMartRequest;
import com.abd.peticionsql.model.DataWarehouseRequest;
import com.abd.peticionsql.model.TableColumnInfo;
import com.abd.peticionsql.model.WarehouseInfo;
import com.abd.peticionsql.model.WarehouseListInfo;
import com.abd.peticionsql.model.WarehouseQueryRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class BaseDatosService {

    @Value("${spring.datasource.url}")
    private String url;
    @Value("${spring.datasource.username}")
    private String user;
    @Value("${spring.datasource.password}")
    private String password;

    public void crearBaseDatos(String nombreBD) throws SQLException {
        String sql = "CREATE DATABASE [" + nombreBD + "]";
        try (Connection conn = DriverManager.getConnection(url, user, password);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    public void eliminarBaseDatos(String nombreBD) throws SQLException {
        eliminarBaseDatosConForzado(nombreBD, true);
    }

    private void eliminarBaseDatosConForzado(String nombreBD, boolean cerrarConexiones) throws SQLException {
        if (cerrarConexiones) {
            cerrarConexionesActivas(nombreBD);
        }

        String sql = "DROP DATABASE [" + nombreBD + "]";
        try (Connection conn = DriverManager.getConnection(url, user, password);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    public String comandoPersonalizado(String nombreBD, String sql) throws SQLException {
        String urlConBD = url + "databaseName=" + nombreBD + ";";
        try (Connection conn = DriverManager.getConnection(urlConBD, user, password);
                Statement stmt = conn.createStatement()) {

            boolean isQuery = stmt.execute(sql);

            if (isQuery) {
                StringBuilder result = new StringBuilder();
                try (ResultSet rs = stmt.getResultSet()) {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        if (i > 1) {
                            result.append(" | ");
                        }
                        result.append(metaData.getColumnName(i));
                    }
                    result.append("\n");
                    result.append("-".repeat(result.length())).append("\n");
                    while (rs.next()) {
                        for (int i = 1; i <= columnCount; i++) {
                            if (i > 1) {
                                result.append(" | ");
                            }
                            String valor = rs.getString(i);
                            result.append(valor != null ? valor : "NULL");
                        }
                        result.append("\n");
                    }
                }
                return result.toString();
            } else {
                int affectedRows = stmt.getUpdateCount();
                return "Filas afectadas: " + affectedRows;
            }
        }
    }

    public List<String> listarBasesDatos() throws SQLException {
        List<String> databases = new ArrayList<>();
        String sql = "SELECT name FROM sys.databases WHERE database_id > 4";

        try (Connection conn = DriverManager.getConnection(url, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                databases.add(rs.getString("name"));
            }
        }
        return databases;
    }

    public List<String> listarTablas(String nombreBD) throws SQLException {
        List<String> tablas = new ArrayList<>();
        String urlConBD = url + "databaseName=" + nombreBD + ";";
        String sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE 'sys%'";

        try (Connection conn = DriverManager.getConnection(urlConBD, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                tablas.add(rs.getString("TABLE_NAME"));
            }
        }
        return tablas;
    }

    public List<ColumnaInfo> listarColumnas(String nombreBD, String nombreTabla) throws SQLException {
        List<ColumnaInfo> columnas = new ArrayList<>();
        String urlConBD = url + "databaseName=" + nombreBD + ";";
        String sql = """
                SELECT
                    c.COLUMN_NAME,
                    c.DATA_TYPE +
                    CASE
                        WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN '(' + CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')'
                        WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NOT NULL THEN '(' + CAST(c.NUMERIC_PRECISION AS VARCHAR) + ',' + CAST(c.NUMERIC_SCALE AS VARCHAR) + ')'
                        WHEN c.NUMERIC_PRECISION IS NOT NULL THEN '(' + CAST(c.NUMERIC_PRECISION AS VARCHAR) + ')'
                        ELSE ''
                    END as TIPO_COMPLETO,
                    CASE WHEN c.IS_NULLABLE = 'YES' THEN 1 ELSE 0 END as ES_NULO,
                    CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as ES_PRIMARIA,
                    CASE WHEN fk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as ES_FORANEA,
                    fk.REFERENCED_TABLE_NAME as TABLA_REFERENCIA,
                    fk.REFERENCED_COLUMN_NAME as COLUMNA_REFERENCIA
                FROM INFORMATION_SCHEMA.COLUMNS c
                LEFT JOIN (
                    SELECT ku.COLUMN_NAME
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                    WHERE tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME                LEFT JOIN (
                    SELECT
                        COL_NAME(fc.parent_object_id, fc.parent_column_id) AS COLUMN_NAME,
                        OBJECT_NAME(fc.referenced_object_id) AS REFERENCED_TABLE_NAME,
                        COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS REFERENCED_COLUMN_NAME
                    FROM sys.foreign_keys AS f
                    INNER JOIN sys.foreign_key_columns AS fc ON f.object_id = fc.constraint_object_id
                    WHERE OBJECT_NAME(fc.parent_object_id) = ?
                ) fk ON c.COLUMN_NAME = fk.COLUMN_NAME
                WHERE c.TABLE_NAME = ?
                ORDER BY c.ORDINAL_POSITION
                """;
        try (Connection conn = DriverManager.getConnection(urlConBD, user, password);
                PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, nombreTabla);
            pstmt.setString(2, nombreTabla);
            pstmt.setString(3, nombreTabla);

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    ColumnaInfo columna = new ColumnaInfo(
                            rs.getString("COLUMN_NAME"),
                            rs.getString("TIPO_COMPLETO"),
                            rs.getBoolean("ES_NULO"),
                            rs.getBoolean("ES_PRIMARIA"),
                            rs.getBoolean("ES_FORANEA"),
                            rs.getString("TABLA_REFERENCIA"),
                            rs.getString("COLUMNA_REFERENCIA"));
                    columnas.add(columna);
                }
            }
        }
        return columnas;
    }

    public List<String> listarVistas(String nombreBD) throws SQLException {
        List<String> vistas = new ArrayList<>();
        String urlConBD = url + "databaseName=" + nombreBD + ";";
        String sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS";

        try (Connection conn = DriverManager.getConnection(urlConBD, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                vistas.add(rs.getString("TABLE_NAME"));
            }
        }
        return vistas;
    }

    public String obtenerDatosBaseDatos(String nombreBD) throws SQLException {
        StringBuilder resultado = new StringBuilder();
        String urlConBD = url + "databaseName=" + nombreBD + ";";

        resultado.append("Nombre de la base de datos: ").append(nombreBD).append("\n\n");

        try (Connection conn = DriverManager.getConnection(urlConBD, user, password)) {

            String sqlTablas = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME != 'sysdiagrams' ORDER BY TABLE_NAME";

            try (Statement stmtTablas = conn.createStatement();
                    ResultSet rsTablas = stmtTablas.executeQuery(sqlTablas)) {

                resultado.append("TABLAS:\n");
                resultado.append("========\n");

                while (rsTablas.next()) {
                    String nombreTabla = rsTablas.getString("TABLE_NAME");
                    resultado.append("• ").append(nombreTabla).append("\n");

                    String sqlColumnas = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? ORDER BY ORDINAL_POSITION";
                    try (PreparedStatement pstmtColumnas = conn.prepareStatement(sqlColumnas)) {
                        pstmtColumnas.setString(1, nombreTabla);
                        try (ResultSet rsColumnas = pstmtColumnas.executeQuery()) {
                            while (rsColumnas.next()) {
                                resultado.append("    - ").append(rsColumnas.getString("COLUMN_NAME")).append("\n");
                            }
                        }
                    }
                    resultado.append("\n");
                }
            }

            String sqlVistas = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS ORDER BY TABLE_NAME";
            try (Statement stmtVistas = conn.createStatement();
                    ResultSet rsVistas = stmtVistas.executeQuery(sqlVistas)) {

                resultado.append("VISTAS:\n");
                resultado.append("=======\n");

                boolean tieneVistas = false;
                while (rsVistas.next()) {
                    tieneVistas = true;
                    resultado.append("• ").append(rsVistas.getString("TABLE_NAME")).append("\n");
                }

                if (!tieneVistas) {
                    resultado.append("No hay vistas en esta base de datos.\n");
                }
                resultado.append("\n");
            }

            String sqlProcedures = "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' ORDER BY ROUTINE_NAME";
            try (Statement stmtProcedures = conn.createStatement();
                    ResultSet rsProcedures = stmtProcedures.executeQuery(sqlProcedures)) {

                resultado.append("STORED PROCEDURES:\n");
                resultado.append("==================\n");

                boolean tieneProcedures = false;
                while (rsProcedures.next()) {
                    tieneProcedures = true;
                    resultado.append("• ").append(rsProcedures.getString("ROUTINE_NAME")).append("\n");
                }

                if (!tieneProcedures) {
                    resultado.append("No hay stored procedures en esta base de datos.\n");
                }
            }
        }

        return resultado.toString();
    }

    public String ejecutarSelect(String nombreBD, String sql) throws SQLException {
        String sqlTrimmed = sql.trim().toUpperCase();
        if (!sqlTrimmed.startsWith("SELECT")) {
            throw new SQLException("Solo se permiten consultas SELECT");
        }

        String urlConBD = url + "databaseName=" + nombreBD + ";";
        StringBuilder jsonResult = new StringBuilder();
        jsonResult.append("[");
        try (Connection conn = DriverManager.getConnection(urlConBD, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            boolean firstRow = true;

            while (rs.next()) {
                if (!firstRow) {
                    jsonResult.append(",");
                }
                firstRow = false;

                jsonResult.append("{");
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) {
                        jsonResult.append(",");
                    }

                    String columnName = metaData.getColumnName(i);
                    String value = rs.getString(i);

                    jsonResult.append("\"").append(columnName).append("\":");

                    if (value == null) {
                        jsonResult.append("null");
                    } else {
                        String escapedValue = value.replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");

                        String columnType = metaData.getColumnTypeName(i).toUpperCase();
                        if (columnType.contains("INT") || columnType.contains("DECIMAL") ||
                                columnType.contains("FLOAT") || columnType.contains("NUMERIC") ||
                                columnType.contains("REAL") || columnType.contains("DOUBLE")) {
                            jsonResult.append(escapedValue);
                        } else {
                            jsonResult.append("\"").append(escapedValue).append("\"");
                        }
                    }
                }
                jsonResult.append("}");
            }
        }

        jsonResult.append("]");
        return jsonResult.toString();
    }

    public List<String> listarStoredProcedures(String nombreBD) throws SQLException {
        List<String> procedures = new ArrayList<>();
        String urlConBD = url + "databaseName=" + nombreBD + ";";
        String sql = "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' ORDER BY ROUTINE_NAME";

        try (Connection conn = DriverManager.getConnection(urlConBD, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                procedures.add(rs.getString("ROUTINE_NAME"));
            }
        }
        return procedures;
    }

    public List<DatabaseInfo> obtenerDatabasesConTablas() throws SQLException {
        List<DatabaseInfo> databases = new ArrayList<>();

        // Primero obtener todas las bases de datos
        List<String> databaseNames = listarBasesDatos();

        for (String dbName : databaseNames) {
            // Excluir bases de datos que terminen en "_warehouse"
            if (dbName.toLowerCase().endsWith("_warehouse")) {
                continue;
            }

            try {
                List<String> tables = listarTablas(dbName);
                DatabaseInfo dbInfo = new DatabaseInfo(dbName, tables);
                databases.add(dbInfo);
            } catch (SQLException e) {
                System.err.println("Error al obtener tablas de la base de datos " + dbName + ": " + e.getMessage());
            }
        }

        return databases;
    }

    public List<TableColumnInfo> obtenerColumnasDeTabla(String database, String table) throws SQLException {
        List<TableColumnInfo> columns = new ArrayList<>();
        String urlConBD = url + "databaseName=" + database + ";";

        String sql = """
                SELECT
                    c.COLUMN_NAME,
                    c.DATA_TYPE +
                    CASE
                        WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN '(' + CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')'
                        WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NOT NULL THEN '(' + CAST(c.NUMERIC_PRECISION AS VARCHAR) + ',' + CAST(c.NUMERIC_SCALE AS VARCHAR) + ')'
                        WHEN c.NUMERIC_PRECISION IS NOT NULL THEN '(' + CAST(c.NUMERIC_PRECISION AS VARCHAR) + ')'
                        ELSE ''
                    END as TIPO_COMPLETO,
                    CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as ES_PRIMARIA
                FROM INFORMATION_SCHEMA.COLUMNS c
                LEFT JOIN (
                    SELECT ku.COLUMN_NAME
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                    WHERE tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
                WHERE c.TABLE_NAME = ?
                ORDER BY c.ORDINAL_POSITION
                """;

        try (Connection conn = DriverManager.getConnection(urlConBD, user, password);
                PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, table);
            pstmt.setString(2, table);

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    TableColumnInfo column = new TableColumnInfo(
                            rs.getString("COLUMN_NAME"),
                            rs.getString("TIPO_COMPLETO"),
                            rs.getBoolean("ES_PRIMARIA"));
                    columns.add(column);
                }
            }
        }

        return columns;
    }

    public Map<String, Object> crearDataWarehouse(DataWarehouseRequest request) throws SQLException {
        Map<String, Object> response = new HashMap<>();
        String warehouseName = null;
        boolean databaseCreated = false;

        try {
            // Validación de datos de entrada
            if (request.getName() == null || request.getName().trim().isEmpty()) {
                throw new SQLException("El nombre del Data Warehouse es requerido");
            }
            if (request.getTableName() == null || request.getTableName().trim().isEmpty()) {
                throw new SQLException("El nombre de la tabla es requerido");
            }
            if (request.getSelectedColumns() == null || request.getSelectedColumns().isEmpty()) {
                throw new SQLException("Debe especificar al menos una columna para el Data Warehouse");
            }

            warehouseName = request.getName().trim();
            if (!warehouseName.toLowerCase().endsWith("_warehouse")) {
                warehouseName += "_warehouse";
            }

            List<String> existingDatabases = listarBasesDatos();
            if (existingDatabases.contains(warehouseName)) {
                throw new SQLException("Ya existe una base de datos con el nombre '" + warehouseName + "'");
            }

            validateSelectedColumns(request.getSelectedColumns());

            if (request.getRelationships() != null && !request.getRelationships().isEmpty()) {
                validateRelationships(request.getRelationships());
            }

            String generatedSQL = generateDynamicSQL(warehouseName, request);

            crearBaseDatos(warehouseName);
            databaseCreated = true;

            executeSQL(generatedSQL);

            Map<String, Object> details = new HashMap<>();
            details.put("tablesProcessed",
                    request.getSelectedTables() != null ? request.getSelectedTables().size() : 0);
            details.put("columnsCreated", request.getSelectedColumns().size());
            details.put("relationshipsApplied",
                    request.getRelationships() != null ? request.getRelationships().size() : 0);

            response.put("warehouseName", warehouseName);
            response.put("message", "Data Warehouse creado exitosamente");
            response.put("sql", generatedSQL);
            response.put("details", details);

        } catch (SQLException e) {
            if (databaseCreated && warehouseName != null) {
                try {
                    System.out.println("Realizando rollback: eliminando base de datos creada '" + warehouseName + "'");
                    eliminarBaseDatos(warehouseName);
                    System.out.println("Rollback completado exitosamente");
                } catch (SQLException rollbackException) {
                    System.err.println("Error durante rollback al eliminar base de datos '" + warehouseName + "': "
                            + rollbackException.getMessage());
                }
            }

            response.put("warehouseName", null);
            response.put("message", "Error al crear Data Warehouse: " + e.getMessage());
            response.put("sql", null);
            response.put("details", null);
        }

        return response;
    }

    private void validateSelectedColumns(List<DataWarehouseRequest.SelectedColumn> selectedColumns)
            throws SQLException {
        for (DataWarehouseRequest.SelectedColumn column : selectedColumns) {
            if (column.getDatabase() == null || column.getDatabase().trim().isEmpty()) {
                throw new SQLException("Todas las columnas deben especificar una base de datos");
            }
            if (column.getTable() == null || column.getTable().trim().isEmpty()) {
                throw new SQLException("Todas las columnas deben especificar una tabla");
            }
            if (column.getColumn() == null || column.getColumn().trim().isEmpty()) {
                throw new SQLException("Todas las columnas deben especificar un nombre de columna");
            }

            List<String> databases = listarBasesDatos();
            if (!databases.contains(column.getDatabase())) {
                throw new SQLException("La base de datos '" + column.getDatabase() + "' no existe");
            }

            List<String> tables = listarTablas(column.getDatabase());
            if (!tables.contains(column.getTable())) {
                throw new SQLException("La tabla '" + column.getTable() + "' no existe en la base de datos '"
                        + column.getDatabase() + "'");
            }

            List<TableColumnInfo> columns = obtenerColumnasDeTabla(column.getDatabase(), column.getTable());
            boolean columnExists = columns.stream()
                    .anyMatch(col -> col.getName().equals(column.getColumn()));
            if (!columnExists) {
                throw new SQLException(
                        "La columna '" + column.getColumn() + "' no existe en la tabla '" + column.getTable() + "'");
            }
        }
    }

    public List<WarehouseInfo> listarDataWarehouses() throws SQLException {
        List<WarehouseInfo> warehouses = new ArrayList<>();

        List<String> databases = listarBasesDatos();

        for (String dbName : databases) {
            if (dbName.toLowerCase().endsWith("_warehouse")) {
                try {
                    List<String> tables = listarTablas(dbName);

                    String created_date = "2024-12-25";
                    int table_count = tables.size();
                    String status = "Activo";

                    WarehouseInfo warehouse = new WarehouseInfo(dbName, created_date, table_count, status);
                    warehouses.add(warehouse);
                } catch (SQLException e) {
                    System.err.println("Error al verificar warehouse " + dbName + ": " + e.getMessage());
                }
            }
        }

        return warehouses;
    }

    public Map<String, Object> eliminarDataWarehouse(String warehouseName) throws SQLException {
        Map<String, Object> response = new HashMap<>();

        try {
            List<String> databases = listarBasesDatos();
            if (!databases.contains(warehouseName)) {
                response.put("success", false);
                response.put("message", "El Data Warehouse '" + warehouseName + "' no existe");
                return response;
            }

            eliminarWarehouseForzadoDirecto(warehouseName);

            response.put("success", true);
            response.put("message", "Data Warehouse '" + warehouseName + "' eliminado exitosamente");

        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Error al eliminar Data Warehouse: " + e.getMessage());
        }

        return response;
    }

    private void cerrarConexionesActivas(String nombreBD) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, user, password);
                Statement stmt = conn.createStatement()) {

            String setSingleUser = "ALTER DATABASE [" + nombreBD + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE";
            stmt.executeUpdate(setSingleUser);

            String setMultiUser = "ALTER DATABASE [" + nombreBD + "] SET MULTI_USER";
            stmt.executeUpdate(setMultiUser);
        }
    }

    public Map<String, Object> ejecutarConsultaWarehouse(WarehouseQueryRequest request) throws SQLException {
        List<String> databases = listarBasesDatos();
        String databaseName = request.getDatabase() != null ? request.getDatabase() : request.getWarehouse();

        if (!databases.contains(databaseName)) {
            throw new SQLException("El Data Warehouse '" + databaseName + "' no existe");
        }

        return ejecutarSelectEstructurado(databaseName, request.getQuery());
    }

    private Map<String, Object> ejecutarSelectEstructurado(String nombreBD, String sql) throws SQLException {
        String sqlTrimmed = sql.trim().toUpperCase();
        if (!sqlTrimmed.startsWith("SELECT")) {
            throw new SQLException("Solo se permiten consultas SELECT");
        }

        String urlConBD = url + "databaseName=" + nombreBD + ";";
        List<String> columns = new ArrayList<>();
        List<List<Object>> rows = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(urlConBD, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                columns.add(metaData.getColumnName(i));
            }

            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    Object value = rs.getObject(i);
                    row.add(value);
                }
                rows.add(row);
            }
        }

        Map<String, Object> response = new HashMap<>();
        response.put("columns", columns);
        response.put("rows", rows);
        response.put("totalRows", rows.size());

        return response;
    }

    private void validateRelationships(List<DataWarehouseRequest.Relationship> relationships) throws SQLException {
        for (DataWarehouseRequest.Relationship relationship : relationships) {
            if (relationship.getTable1() == null || relationship.getTable2() == null) {
                throw new SQLException("Las relaciones deben especificar table1 y table2");
            }
            if (relationship.getType() == null || relationship.getType().trim().isEmpty()) {
                throw new SQLException("Las relaciones deben especificar un tipo (INNER_JOIN, LEFT_JOIN, etc.)");
            }

            validateTableReference(relationship.getTable1());
            validateTableReference(relationship.getTable2());
        }
    }

    private void validateTableReference(DataWarehouseRequest.TableRef tableRef) throws SQLException {
        if (tableRef.getDatabase() == null || tableRef.getDatabase().trim().isEmpty()) {
            throw new SQLException("La referencia de tabla debe especificar una base de datos");
        }
        if (tableRef.getTable() == null || tableRef.getTable().trim().isEmpty()) {
            throw new SQLException("La referencia de tabla debe especificar una tabla");
        }
        if (tableRef.getColumn() == null || tableRef.getColumn().trim().isEmpty()) {
            throw new SQLException("La referencia de tabla debe especificar una columna");
        }

        List<String> databases = listarBasesDatos();
        if (!databases.contains(tableRef.getDatabase())) {
            throw new SQLException("La base de datos '" + tableRef.getDatabase() + "' no existe");
        }

        List<String> tables = listarTablas(tableRef.getDatabase());
        if (!tables.contains(tableRef.getTable())) {
            throw new SQLException("La tabla '" + tableRef.getTable() + "' no existe en la base de datos '"
                    + tableRef.getDatabase() + "'");
        }

        List<TableColumnInfo> columns = obtenerColumnasDeTabla(tableRef.getDatabase(), tableRef.getTable());
        boolean columnExists = columns.stream()
                .anyMatch(col -> col.getName().equals(tableRef.getColumn()));
        if (!columnExists) {
            throw new SQLException(
                    "La columna '" + tableRef.getColumn() + "' no existe en la tabla '" + tableRef.getTable() + "'");
        }
    }

    private String generateDynamicSQL(String warehouseName, DataWarehouseRequest request) throws SQLException {
        StringBuilder sql = new StringBuilder();

        try {
            sql.append("USE [").append(warehouseName).append("]; ");

            Map<String, String> tableAliasMap = buildTableAliasMap(request);

            sql.append("SELECT ");

            List<String> selectColumns = new ArrayList<>();
            for (DataWarehouseRequest.SelectedColumn column : request.getSelectedColumns()) {
                String tableKey = column.getDatabase() + "." + column.getTable();
                String tableAlias = tableAliasMap.get(tableKey);

                if (tableAlias == null) {
                    throw new SQLException("No se encontró alias para la tabla: " + tableKey);
                }

                String columnAlias = column.getAlias() != null && !column.getAlias().trim().isEmpty()
                        ? column.getAlias()
                        : column.getColumn();

                validateColumnExists(column.getDatabase(), column.getTable(), column.getColumn());

                selectColumns.add(tableAlias + ".[" + column.getColumn() + "] AS [" + columnAlias + "]");
            }

            sql.append(String.join(", ", selectColumns));

            sql.append(" INTO [").append(request.getTableName()).append("] ");

            String fromAndJoins = buildFromAndJoins(request, tableAliasMap);
            sql.append(fromAndJoins);

            sql.append(";");

        } catch (SQLException e) {
            throw new SQLException("Error al generar SQL dinámico: " + e.getMessage(), e);
        }

        return sql.toString();
    }

    private Map<String, String> buildTableAliasMap(DataWarehouseRequest request) {
        Map<String, String> aliasMap = new HashMap<>();

        if (request.getSelectedTables() != null) {
            for (DataWarehouseRequest.SelectedTable selectedTable : request.getSelectedTables()) {
                String tableKey = selectedTable.getDatabase() + "." + selectedTable.getTable();
                String alias = selectedTable.getAlias() != null && !selectedTable.getAlias().trim().isEmpty()
                        ? selectedTable.getAlias()
                        : selectedTable.getTable().substring(0, Math.min(3, selectedTable.getTable().length()));
                aliasMap.put(tableKey, alias);
            }
        }

        for (DataWarehouseRequest.SelectedColumn column : request.getSelectedColumns()) {
            String tableKey = column.getDatabase() + "." + column.getTable();
            if (!aliasMap.containsKey(tableKey)) {
                String alias = column.getTable().substring(0, Math.min(3, column.getTable().length()));
                aliasMap.put(tableKey, alias);
            }
        }

        return aliasMap;
    }

    private void validateColumnExists(String database, String table, String column) throws SQLException {
        List<TableColumnInfo> columns = obtenerColumnasDeTabla(database, table);
        boolean exists = columns.stream().anyMatch(col -> col.getName().equals(column));
        if (!exists) {
            throw new SQLException("La columna '" + column + "' no existe en la tabla '" + table
                    + "' de la base de datos '" + database + "'");
        }
    }

    private String buildFromAndJoins(DataWarehouseRequest request, Map<String, String> tableAliasMap)
            throws SQLException {
        StringBuilder fromClause = new StringBuilder();

        DataWarehouseRequest.SelectedColumn firstColumn = request.getSelectedColumns().get(0);
        String mainTableKey = firstColumn.getDatabase() + "." + firstColumn.getTable();
        String mainAlias = tableAliasMap.get(mainTableKey);

        fromClause.append("FROM [").append(firstColumn.getDatabase()).append("].[dbo].[")
                .append(firstColumn.getTable()).append("] ").append(mainAlias);

        if (request.getRelationships() != null && !request.getRelationships().isEmpty()) {
            Set<String> joinedTables = new HashSet<>();
            joinedTables.add(mainTableKey);

            for (DataWarehouseRequest.Relationship relationship : request.getRelationships()) {
                String joinType = mapJoinType(relationship.getType());

                DataWarehouseRequest.TableRef table1 = relationship.getTable1();
                DataWarehouseRequest.TableRef table2 = relationship.getTable2();

                String table1Key = table1.getDatabase() + "." + table1.getTable();
                String table2Key = table2.getDatabase() + "." + table2.getTable();

                validateJoinCompatibility(table1, table2);

                String joinCondition;
                if (joinedTables.contains(table1Key)) {
                    String joinAlias = tableAliasMap.get(table2Key);
                    joinCondition = tableAliasMap.get(table1Key) + ".[" + table1.getColumn() + "] = "
                            + joinAlias + ".[" + table2.getColumn() + "]";
                    fromClause.append(" ").append(joinType).append(" [").append(table2.getDatabase())
                            .append("].[dbo].[").append(table2.getTable()).append("] ").append(joinAlias)
                            .append(" ON ").append(joinCondition);
                    joinedTables.add(table2Key);
                } else if (joinedTables.contains(table2Key)) {
                    // Unir table1
                    String joinAlias = tableAliasMap.get(table1Key);
                    joinCondition = tableAliasMap.get(table2Key) + ".[" + table2.getColumn() + "] = "
                            + joinAlias + ".[" + table1.getColumn() + "]";
                    fromClause.append(" ").append(joinType).append(" [").append(table1.getDatabase())
                            .append("].[dbo].[").append(table1.getTable()).append("] ").append(joinAlias)
                            .append(" ON ").append(joinCondition);
                    joinedTables.add(table1Key);
                } else {
                    throw new SQLException("Relación inválida: ninguna de las tablas (" + table1Key + ", " + table2Key
                            + ") está incluida en el FROM principal");
                }
            }
        }

        return fromClause.toString();
    }

    private void validateJoinCompatibility(DataWarehouseRequest.TableRef table1, DataWarehouseRequest.TableRef table2)
            throws SQLException {
        List<TableColumnInfo> columns1 = obtenerColumnasDeTabla(table1.getDatabase(), table1.getTable());
        List<TableColumnInfo> columns2 = obtenerColumnasDeTabla(table2.getDatabase(), table2.getTable());

        TableColumnInfo col1 = columns1.stream()
                .filter(col -> col.getName().equals(table1.getColumn()))
                .findFirst()
                .orElseThrow(() -> new SQLException(
                        "Columna '" + table1.getColumn() + "' no encontrada en tabla " + table1.getTable()));

        TableColumnInfo col2 = columns2.stream()
                .filter(col -> col.getName().equals(table2.getColumn()))
                .findFirst()
                .orElseThrow(() -> new SQLException(
                        "Columna '" + table2.getColumn() + "' no encontrada en tabla " + table2.getTable()));

        String type1 = col1.getType().toUpperCase();
        String type2 = col2.getType().toUpperCase();

        String baseType1 = type1.split("\\(")[0].trim();
        String baseType2 = type2.split("\\(")[0].trim();

        Set<String> numericTypes = Set.of("INT", "BIGINT", "SMALLINT", "TINYINT", "DECIMAL", "NUMERIC", "FLOAT",
                "REAL");
        Set<String> stringTypes = Set.of("VARCHAR", "NVARCHAR", "CHAR", "NCHAR", "TEXT", "NTEXT");
        Set<String> dateTypes = Set.of("DATE", "DATETIME", "DATETIME2", "SMALLDATETIME", "TIME", "TIMESTAMP");

        boolean compatible = false;
        if (baseType1.equals(baseType2)) {
            compatible = true;
        } else if (numericTypes.contains(baseType1) && numericTypes.contains(baseType2)) {
            compatible = true;
        } else if (stringTypes.contains(baseType1) && stringTypes.contains(baseType2)) {
            compatible = true;
        } else if (dateTypes.contains(baseType1) && dateTypes.contains(baseType2)) {
            compatible = true;
        }

        if (!compatible) {
            throw new SQLException("Tipos de columnas incompatibles para JOIN: " +
                    table1.getTable() + "." + table1.getColumn() + " (" + type1 + ") vs " +
                    table2.getTable() + "." + table2.getColumn() + " (" + type2 + ")");
        }
    }

    private String mapJoinType(String joinType) {
        switch (joinType.toUpperCase()) {
            case "INNER_JOIN":
                return "INNER JOIN";
            case "LEFT_JOIN":
                return "LEFT JOIN";
            case "RIGHT_JOIN":
                return "RIGHT JOIN";
            case "FULL_JOIN":
                return "FULL OUTER JOIN";
            default:
                return "INNER JOIN";
        }
    }

    private void executeSQL(String sql) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, user, password);
                Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    public List<WarehouseListInfo> listarDataWarehousesConTablas() throws SQLException {
        List<WarehouseListInfo> warehouses = new ArrayList<>();

        List<String> databases = listarBasesDatos();

        for (String dbName : databases) {
            if (dbName.toLowerCase().endsWith("_warehouse")) {
                try {

                    List<String> tables = listarTablas(dbName);

                    WarehouseListInfo warehouse = new WarehouseListInfo(dbName, tables);
                    warehouses.add(warehouse);
                } catch (SQLException e) {

                    System.err.println("Error al obtener tablas del warehouse " + dbName + ": " + e.getMessage());
                }
            }
        }

        return warehouses;
    }

    private void eliminarWarehouseForzadoDirecto(String nombreBD) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, user, password);
                Statement stmt = conn.createStatement()) {

            String sqlForzado = String.format("""
                    -- Matar todas las conexiones activas
                    DECLARE @sql NVARCHAR(MAX) = '';
                    SELECT @sql = @sql + 'KILL ' + CAST(session_id AS NVARCHAR(10)) + '; '
                    FROM sys.dm_exec_sessions
                    WHERE database_id = DB_ID('%s') AND session_id != @@SPID;
                    EXEC sp_executesql @sql;

                    -- Poner en modo SINGLE_USER y eliminar inmediatamente
                    ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [%s];
                    """, nombreBD, nombreBD, nombreBD);

            stmt.execute(sqlForzado);
        }
    }

    public List<Map<String, Object>> obtenerColumnasDeWarehouse(String warehouseName) throws SQLException {
        List<Map<String, Object>> allColumns = new ArrayList<>();

        List<String> databases = listarBasesDatos();
        if (!databases.contains(warehouseName)) {
            throw new SQLException("El Data Warehouse '" + warehouseName + "' no existe");
        }

        String urlConBD = url + "databaseName=" + warehouseName + ";";

        String sql = """
                SELECT
                    c.TABLE_NAME,
                    c.COLUMN_NAME,
                    c.DATA_TYPE +
                    CASE
                        WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN '(' + CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')'
                        WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NOT NULL THEN '(' + CAST(c.NUMERIC_PRECISION AS VARCHAR) + ',' + CAST(c.NUMERIC_SCALE AS VARCHAR) + ')'
                        WHEN c.NUMERIC_PRECISION IS NOT NULL THEN '(' + CAST(c.NUMERIC_PRECISION AS VARCHAR) + ')'
                        ELSE ''
                    END as TIPO_COMPLETO,
                    CASE WHEN c.IS_NULLABLE = 'YES' THEN 1 ELSE 0 END as ES_NULO,
                    CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as ES_PRIMARIA
                FROM INFORMATION_SCHEMA.COLUMNS c
                INNER JOIN INFORMATION_SCHEMA.TABLES t ON c.TABLE_NAME = t.TABLE_NAME
                LEFT JOIN (
                    SELECT tc.TABLE_NAME, ku.COLUMN_NAME
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                ) pk ON c.TABLE_NAME = pk.TABLE_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME
                WHERE t.TABLE_TYPE = 'BASE TABLE' AND c.TABLE_NAME NOT LIKE 'sys%'
                ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
                """;

        try (Connection conn = DriverManager.getConnection(urlConBD, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> column = new HashMap<>();
                column.put("table", rs.getString("TABLE_NAME"));
                column.put("name", rs.getString("COLUMN_NAME"));
                column.put("type", rs.getString("TIPO_COMPLETO"));
                column.put("nullable", rs.getBoolean("ES_NULO"));
                column.put("primaryKey", rs.getBoolean("ES_PRIMARIA"));
                allColumns.add(column);
            }
        }

        return allColumns;
    }

    public Map<String, Object> crearDataMart(DataMartRequest request) throws SQLException {
        Map<String, Object> response = new HashMap<>();
        long startTime = System.currentTimeMillis();

        try {

            if (request.getName() == null || request.getName().trim().isEmpty()) {
                throw new SQLException("El nombre del Data Mart es requerido");
            }
            if (request.getSourceWarehouse() == null || request.getSourceWarehouse().trim().isEmpty()) {
                throw new SQLException("El warehouse de origen es requerido");
            }
            if (request.getSelectedColumns() == null || request.getSelectedColumns().isEmpty()) {
                throw new SQLException("Debe especificar al menos una columna para el Data Mart");
            }

            String dataMartName = request.getName().trim();
            String sourceWarehouse = request.getSourceWarehouse().trim();

            List<String> databases = listarBasesDatos();
            if (!databases.contains(sourceWarehouse)) {
                throw new SQLException("El warehouse de origen '" + sourceWarehouse + "' no existe");
            }

            // Determinar tabla origen
            String sourceTable = determineSourceTable(sourceWarehouse, request);

            validateDataMartColumnsInTable(sourceWarehouse, sourceTable, request.getSelectedColumns());

            String generatedSQL = generateDataMartSQL(dataMartName, sourceWarehouse, sourceTable,
                    request.getSelectedColumns());

            int rowsAffected = executeDataMartSQL(sourceWarehouse, generatedSQL);

            long endTime = System.currentTimeMillis();
            double executionTime = (endTime - startTime) / 1000.0;

            Map<String, Object> details = new HashMap<>();
            details.put("rowsAffected", rowsAffected);
            details.put("columnsCreated", request.getSelectedColumns().size());
            details.put("executionTime", String.format("%.2fs", executionTime));
            details.put("sourceTable", sourceTable);

            response.put("success", true);
            response.put("message",
                    "Data Mart '" + dataMartName + "' creado exitosamente desde la tabla '" + sourceTable + "'");
            response.put("dataMartName", dataMartName);
            response.put("sql", generatedSQL);
            response.put("details", details);

        } catch (SQLException e) {
            response.put("success", false);
            response.put("message", "Error al crear Data Mart: " + e.getMessage());
            response.put("dataMartName", null);
            response.put("sql", null);
            response.put("details", null);
        }

        return response;
    }

    private void validateDataMartColumnsInTable(String warehouse, String table,
            List<DataMartRequest.SelectedColumn> selectedColumns)
            throws SQLException {

        List<TableColumnInfo> tableColumns = obtenerColumnasDeTabla(warehouse, table);

        for (DataMartRequest.SelectedColumn selectedCol : selectedColumns) {
            boolean columnExists = tableColumns.stream()
                    .anyMatch(col -> col.getName().equals(selectedCol.getName()));

            if (!columnExists) {
                throw new SQLException(
                        "La columna '" + selectedCol.getName() + "' no existe en la tabla '" + table
                                + "' del warehouse '" + warehouse + "'");
            }
        }
    }

    private String determineSourceTable(String warehouse, DataMartRequest request) throws SQLException {

        if (request.getSourceTable() != null && !request.getSourceTable().trim().isEmpty()) {
            String sourceTable = request.getSourceTable().trim();

            // Verificar que la tabla existe
            List<String> tables = listarTablas(warehouse);
            if (!tables.contains(sourceTable)) {
                throw new SQLException("La tabla origen especificada '" + sourceTable + "' no existe en el warehouse '"
                        + warehouse + "'");
            }

            return sourceTable;
        }

        List<String> tables = listarTablas(warehouse);

        if (tables.isEmpty()) {
            throw new SQLException("El warehouse '" + warehouse + "' no tiene tablas disponibles");
        }

        String[] priorityNames = { "datos_consolidados", "warehouse_data", "consolidated_data", "main_data", "data" };

        for (String priorityName : priorityNames) {
            if (tables.contains(priorityName)) {
                return priorityName;
            }
        }

        String selectedTable = tables.get(0);

        if (tables.size() > 1) {
            throw new SQLException("El warehouse '" + warehouse + "' tiene múltiples tablas (" +
                    String.join(", ", tables) + "). Por favor, especifique la tabla origen en el campo 'sourceTable'.");
        }

        return selectedTable;
    }

    private String generateDataMartSQL(String dataMartName, String sourceWarehouse, String sourceTable,
            List<DataMartRequest.SelectedColumn> selectedColumns) {
        StringBuilder sql = new StringBuilder();

        sql.append("SELECT ");
        List<String> selectColumns = new ArrayList<>();

        for (DataMartRequest.SelectedColumn column : selectedColumns) {
            String columnSQL = "[" + column.getName() + "]";
            if (column.getAlias() != null && !column.getAlias().trim().isEmpty()) {
                columnSQL += " AS [" + column.getAlias() + "]";
            }
            selectColumns.add(columnSQL);
        }

        sql.append(String.join(", ", selectColumns));
        sql.append(" INTO [").append(dataMartName).append("]");
        sql.append(" FROM [").append(sourceWarehouse).append("].[dbo].[").append(sourceTable).append("]");

        return sql.toString();
    }

    private int executeDataMartSQL(String warehouse, String sql) throws SQLException {
        String urlConBD = url + "databaseName=" + warehouse + ";";

        try (Connection conn = DriverManager.getConnection(urlConBD, user, password);
                Statement stmt = conn.createStatement()) {

            stmt.execute(sql);
            return stmt.getUpdateCount();
        }
    }
}