package com.abd.peticionsql.services;

import com.abd.peticionsql.model.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;

@Service
public class CuboOlapService {

    @Value("${spring.datasource.url}")
    private String url;
    @Value("${spring.datasource.username}")
    private String user;
    @Value("${spring.datasource.password}")
    private String password;

    /**
     * Ejecuta una consulta PIVOT dinámica para el cubo OLAP
     */
    public CuboOlapResponse ejecutarCuboOlap(CuboOlapRequest request) throws SQLException {
        String connectionUrl = buildConnectionUrl(request.getBaseDatos());

        try (Connection conn = DriverManager.getConnection(connectionUrl, user, password)) {
            // Primero obtenemos los valores únicos de la dimensión Y para construir el
            // PIVOT
            List<String> valoresDimensionY = obtenerValoresUnicos(conn, request.getTabla(), request.getDimensionY());

            // Construimos la consulta PIVOT dinámica
            String pivotQuery = construirConsultaPivot(request, valoresDimensionY);

            // Ejecutamos la consulta
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(pivotQuery)) {

                return procesarResultadoPivot(rs, request.getDimensionX(), valoresDimensionY);
            }
        }
    }

    /**
     * Obtiene valores únicos de un campo para usar como filtro
     */
    public ValoresFiltroResponse obtenerValoresFiltro(ValoresFiltroRequest request) throws SQLException {
        String connectionUrl = buildConnectionUrl(request.getBaseDatos());
        String sql = "SELECT DISTINCT [" + request.getCampo() + "] FROM [" + request.getTabla() + "] ORDER BY ["
                + request.getCampo() + "]";

        List<String> valores = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(connectionUrl, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String valor = rs.getString(1);
                if (valor != null) {
                    valores.add(valor);
                }
            }
        }

        return new ValoresFiltroResponse(valores);
    }

    /**
     * Guarda una vista del cubo OLAP en la base de datos
     */
    public GuardarVistaResponse guardarVista(GuardarVistaRequest request) throws SQLException {
        String connectionUrl = buildConnectionUrl(request.getBaseDatos());

        try (Connection conn = DriverManager.getConnection(connectionUrl, user, password)) {
            // Crear tabla de metadatos si no existe
            crearTablaMetadatosSiNoExiste(conn);

            // Obtener valores únicos de la dimensión Y
            List<String> valoresDimensionY = obtenerValoresUnicos(conn, request.getTabla(), request.getDimensionY());

            // Construir la consulta para crear la vista
            String createViewSql = construirConsultaCrearVista(request, valoresDimensionY);

            // Ejecutar creación de vista
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createViewSql);
            }

            // Guardar metadatos de la vista
            guardarMetadatosVista(conn, request, valoresDimensionY);

            return new GuardarVistaResponse(true, "Vista creada exitosamente", request.getNombreVista());
        }
    }

    /**
     * Consulta una vista existente del cubo OLAP
     */
    public CuboOlapResponse consultarVista(ConsultarVistaRequest request) throws SQLException {
        String connectionUrl = buildConnectionUrl(request.getBaseDatos());

        try (Connection conn = DriverManager.getConnection(connectionUrl, user, password)) {
            // Obtener metadatos de la vista
            InfoVistaResponse infoVista = obtenerInfoVista(conn, request.getVista());

            // Construir consulta
            StringBuilder sql = new StringBuilder("SELECT * FROM [" + request.getVista() + "]");

            if (request.getValorFiltro() != null && !request.getValorFiltro().trim().isEmpty()
                    && infoVista.getCampoFiltro() != null) {
                sql.append(" WHERE [").append(infoVista.getCampoFiltro()).append("] = '")
                        .append(request.getValorFiltro()).append("'");
            }

            // Ejecutar consulta
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(sql.toString())) {

                return procesarResultadoPivot(rs, infoVista.getDimensionX(), infoVista.getColumnas());
            }
        }
    }

    /**
     * Obtiene información de metadatos de una vista
     */
    public InfoVistaResponse obtenerInfoVista(InfoVistaRequest request) throws SQLException {
        String connectionUrl = buildConnectionUrl(request.getBaseDatos());

        try (Connection conn = DriverManager.getConnection(connectionUrl, user, password)) {
            return obtenerInfoVista(conn, request.getVista());
        }
    }

    /**
     * Obtiene valores únicos del campo filtro de una vista
     */
    public ValoresFiltroResponse obtenerValoresFiltroVista(ValoresFiltroVistaRequest request) throws SQLException {
        String connectionUrl = buildConnectionUrl(request.getBaseDatos());
        String sql = "SELECT DISTINCT [" + request.getCampo() + "] FROM [" + request.getVista() + "] ORDER BY ["
                + request.getCampo() + "]";

        List<String> valores = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(connectionUrl, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String valor = rs.getString(1);
                if (valor != null) {
                    valores.add(valor);
                }
            }
        }

        return new ValoresFiltroResponse(valores);
    }

    // Métodos auxiliares privados

    private String buildConnectionUrl(String baseDatos) {
        String baseUrl = url.substring(0, url.lastIndexOf(';'));
        return baseUrl + ";databaseName=" + baseDatos;
    }

    private List<String> obtenerValoresUnicos(Connection conn, String tabla, String campo) throws SQLException {
        String sql = "SELECT DISTINCT [" + campo + "] FROM [" + tabla + "] ORDER BY [" + campo + "]";
        List<String> valores = new ArrayList<>();

        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String valor = rs.getString(1);
                if (valor != null) {
                    valores.add(valor);
                }
            }
        }

        return valores;
    }

    private String construirConsultaPivot(CuboOlapRequest request, List<String> valoresDimensionY) {
        StringBuilder sql = new StringBuilder();

        // Construir la lista de columnas del PIVOT
        StringBuilder pivotColumns = new StringBuilder();
        StringBuilder selectColumns = new StringBuilder();

        for (int i = 0; i < valoresDimensionY.size(); i++) {
            String valor = valoresDimensionY.get(i);
            String columnaSegura = "[" + valor + "]";

            if (i > 0) {
                pivotColumns.append(", ");
                selectColumns.append(", ");
            }
            pivotColumns.append(columnaSegura);
            selectColumns.append("ISNULL(").append(columnaSegura).append(", 0) AS ").append(columnaSegura);
        }

        // Construir la consulta principal
        sql.append("SELECT [").append(request.getDimensionX()).append("], ");
        sql.append(selectColumns.toString());
        sql.append(" FROM (");
        sql.append("SELECT [").append(request.getDimensionX()).append("], ");
        sql.append("[").append(request.getDimensionY()).append("], ");
        sql.append(request.getTipoAgregacion()).append("([").append(request.getCampoValores()).append("]) as total ");
        sql.append("FROM [").append(request.getTabla()).append("]");

        // Añadir filtro si existe
        if (request.getCampoFiltro() != null && !request.getCampoFiltro().trim().isEmpty()
                && request.getValorFiltro() != null && !request.getValorFiltro().trim().isEmpty()) {
            sql.append(" WHERE [").append(request.getCampoFiltro()).append("] = '").append(request.getValorFiltro())
                    .append("'");
        }

        sql.append(" GROUP BY [").append(request.getDimensionX()).append("], [").append(request.getDimensionY())
                .append("]");
        sql.append(") AS src ");
        sql.append("PIVOT (");
        sql.append("SUM(total) FOR [").append(request.getDimensionY()).append("] IN (").append(pivotColumns.toString())
                .append(")");
        sql.append(") AS p ");
        sql.append("ORDER BY [").append(request.getDimensionX()).append("]");

        return sql.toString();
    }

    private String construirConsultaCrearVista(GuardarVistaRequest request, List<String> valoresDimensionY) {
        StringBuilder sql = new StringBuilder();

        // Construir la lista de columnas del PIVOT
        StringBuilder pivotColumns = new StringBuilder();
        StringBuilder selectColumns = new StringBuilder();

        for (int i = 0; i < valoresDimensionY.size(); i++) {
            String valor = valoresDimensionY.get(i);
            String columnaSegura = "[" + valor + "]";

            if (i > 0) {
                pivotColumns.append(", ");
                selectColumns.append(", ");
            }
            pivotColumns.append(columnaSegura);
            selectColumns.append("ISNULL(").append(columnaSegura).append(", 0) AS ").append(columnaSegura);
        }

        // Construir la consulta CREATE VIEW
        sql.append("CREATE VIEW [").append(request.getNombreVista()).append("] AS ");
        sql.append("SELECT [").append(request.getDimensionX()).append("], ");
        sql.append(selectColumns.toString());

        // Incluir campo filtro si existe
        if (request.getCampoFiltro() != null && !request.getCampoFiltro().trim().isEmpty()) {
            sql.append(", [").append(request.getCampoFiltro()).append("]");
        }

        sql.append(" FROM (");
        sql.append("SELECT [").append(request.getDimensionX()).append("], ");
        sql.append("[").append(request.getDimensionY()).append("], ");

        if (request.getCampoFiltro() != null && !request.getCampoFiltro().trim().isEmpty()) {
            sql.append("[").append(request.getCampoFiltro()).append("], ");
        }

        sql.append(request.getTipoAgregacion()).append("([").append(request.getCampoValores()).append("]) as total ");
        sql.append("FROM [").append(request.getTabla()).append("] ");
        sql.append("GROUP BY [").append(request.getDimensionX()).append("], [").append(request.getDimensionY())
                .append("]");

        if (request.getCampoFiltro() != null && !request.getCampoFiltro().trim().isEmpty()) {
            sql.append(", [").append(request.getCampoFiltro()).append("]");
        }

        sql.append(") AS src ");
        sql.append("PIVOT (");
        sql.append("SUM(total) FOR [").append(request.getDimensionY()).append("] IN (").append(pivotColumns.toString())
                .append(")");
        sql.append(") AS p");

        return sql.toString();
    }

    private CuboOlapResponse procesarResultadoPivot(ResultSet rs, String dimensionX, List<String> columnas)
            throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        List<Map<String, Object>> filas = new ArrayList<>();
        List<String> columnasRespuesta = new ArrayList<>();

        // Construir lista de columnas (excluyendo la dimensión X)
        for (int i = 2; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            columnasRespuesta.add(columnName);
        }

        // Procesar filas
        while (rs.next()) {
            // Usar LinkedHashMap para preservar el orden de inserción
            Map<String, Object> fila = new LinkedHashMap<>();

            // Añadir valor de dimensión X PRIMERO
            fila.put(dimensionX, rs.getObject(1));

            // Añadir valores de las columnas pivotadas
            for (int i = 2; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                Object value = rs.getObject(i);
                fila.put(columnName, value != null ? value : 0);
            }

            filas.add(fila);
        }

        return new CuboOlapResponse(columnasRespuesta, filas, dimensionX);
    }

    private void crearTablaMetadatosSiNoExiste(Connection conn) throws SQLException {
        String sql = """
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='cubo_olap_vistas' AND xtype='U')
                CREATE TABLE cubo_olap_vistas (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    nombre_vista VARCHAR(255) NOT NULL,
                    base_datos VARCHAR(255) NOT NULL,
                    tabla_origen VARCHAR(255) NOT NULL,
                    dimension_x VARCHAR(255) NOT NULL,
                    dimension_y VARCHAR(255) NOT NULL,
                    campo_valores VARCHAR(255) NOT NULL,
                    tipo_agregacion VARCHAR(10) NOT NULL,
                    campo_filtro VARCHAR(255),
                    columnas_pivot TEXT NOT NULL,
                    fecha_creacion DATETIME DEFAULT GETDATE(),
                    UNIQUE(nombre_vista, base_datos)
                )
                """;

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    private void guardarMetadatosVista(Connection conn, GuardarVistaRequest request, List<String> valoresDimensionY)
            throws SQLException {
        String columnasPivot = String.join(",", valoresDimensionY);

        String sql = """
                INSERT INTO cubo_olap_vistas
                (nombre_vista, base_datos, tabla_origen, dimension_x, dimension_y, campo_valores, tipo_agregacion, campo_filtro, columnas_pivot)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, request.getNombreVista());
            pstmt.setString(2, request.getBaseDatos());
            pstmt.setString(3, request.getTabla());
            pstmt.setString(4, request.getDimensionX());
            pstmt.setString(5, request.getDimensionY());
            pstmt.setString(6, request.getCampoValores());
            pstmt.setString(7, request.getTipoAgregacion());
            pstmt.setString(8, request.getCampoFiltro());
            pstmt.setString(9, columnasPivot);
            pstmt.executeUpdate();
        }
    }

    private InfoVistaResponse obtenerInfoVista(Connection conn, String nombreVista) throws SQLException {
        String sql = "SELECT campo_filtro, dimension_x, dimension_y, columnas_pivot FROM cubo_olap_vistas WHERE nombre_vista = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, nombreVista);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    String campoFiltro = rs.getString("campo_filtro");
                    String dimensionX = rs.getString("dimension_x");
                    String dimensionY = rs.getString("dimension_y");
                    String columnasPivotStr = rs.getString("columnas_pivot");

                    List<String> columnas = Arrays.asList(columnasPivotStr.split(","));

                    return new InfoVistaResponse(campoFiltro, dimensionX, dimensionY, columnas);
                } else {
                    throw new SQLException("Vista no encontrada: " + nombreVista);
                }
            }
        }
    }
}
