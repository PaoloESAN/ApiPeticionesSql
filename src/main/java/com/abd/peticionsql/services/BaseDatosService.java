package com.abd.peticionsql.services;

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

        // Título de la base de datos
        resultado.append("Nombre de la base de datos: ").append(nombreBD).append("\n\n");

        try (Connection conn = DriverManager.getConnection(urlConBD, user, password)) {

            // Obtener tablas y sus columnas
            String sqlTablas = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME != 'sysdiagrams' ORDER BY TABLE_NAME";

            try (Statement stmtTablas = conn.createStatement();
                    ResultSet rsTablas = stmtTablas.executeQuery(sqlTablas)) {

                resultado.append("TABLAS:\n");
                resultado.append("========\n");

                while (rsTablas.next()) {
                    String nombreTabla = rsTablas.getString("TABLE_NAME");
                    resultado.append("• ").append(nombreTabla).append("\n");

                    // Obtener columnas de cada tabla
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

            // Obtener vistas
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
                    resultado.append("No hay vistas.\n");
                }
            }
        }

        return resultado.toString();
    }

}