package com.abd.peticionsql.services;

import com.abd.peticionsql.model.ColumnaInfo;
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
import java.util.List;

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
        String sql = "SELECT name FROM sys.databases WHERE database_id > 4"; // Excluye bases de datos del sistema

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

}