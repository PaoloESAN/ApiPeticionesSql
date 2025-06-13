package com.abd.peticionsql.services;

import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Service
public class BaseDatosService {

    private static final String url = "jdbc:sqlserver://localhost\\SQLEXPRESS;trustServerCertificate=true;";
    private static final String user = "sa";
    private static final String password = "123456789";

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
        String sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'";

        try (Connection conn = DriverManager.getConnection(urlConBD, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                tablas.add(rs.getString("TABLE_NAME"));
            }
        }
        return tablas;
    }

    public List<String> listarColumnas(String nombreBD, String nombreTabla) throws SQLException {
        List<String> columnas = new ArrayList<>();
        String urlConBD = url + "databaseName=" + nombreBD + ";";
        String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + nombreTabla
                + "' ORDER BY ORDINAL_POSITION";

        try (Connection conn = DriverManager.getConnection(urlConBD, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                columnas.add(rs.getString("COLUMN_NAME"));
            }
        }
        return columnas;
    }

}