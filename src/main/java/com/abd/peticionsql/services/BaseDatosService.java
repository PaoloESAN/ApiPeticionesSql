package com.abd.peticionsql.services;

import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
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

    public void comandoPersonalizado(String nombreBD, String sql) throws SQLException {
        String urlConBD = url + "databaseName=" + nombreBD + ";";
        try (Connection conn = DriverManager.getConnection(urlConBD, user, password);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
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
}