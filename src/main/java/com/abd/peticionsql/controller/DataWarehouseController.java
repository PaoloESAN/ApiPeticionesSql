package com.abd.peticionsql.controller;

import com.abd.peticionsql.model.DatabaseInfo;
import com.abd.peticionsql.model.DataWarehouseRequest;
import com.abd.peticionsql.model.TableColumnInfo;
import com.abd.peticionsql.model.WarehouseListInfo;
import com.abd.peticionsql.model.WarehouseQueryRequest;
import com.abd.peticionsql.services.BaseDatosService;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.sql.SQLException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/datawarehouse")
public class DataWarehouseController {

    private final BaseDatosService baseDatosService;

    public DataWarehouseController(BaseDatosService baseDatosService) {
        this.baseDatosService = baseDatosService;
    }

    @GetMapping("/databases-tables")
    public ResponseEntity<?> getDatabasesWithTables() {
        try {
            List<DatabaseInfo> databases = baseDatosService.obtenerDatabasesConTablas();
            Map<String, Object> response = new HashMap<>();
            response.put("databases", databases);
            return ResponseEntity.ok(response);
        } catch (SQLException e) {
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error al obtener bases de datos y tablas: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @GetMapping("/table-columns")
    public ResponseEntity<?> getTableColumns(
            @RequestParam(name = "database", required = true) String database,
            @RequestParam(name = "table", required = true) String table) {
        try {
            List<TableColumnInfo> columns = baseDatosService.obtenerColumnasDeTabla(database, table);
            Map<String, Object> response = new HashMap<>();
            response.put("columns", columns);
            return ResponseEntity.ok(response);
        } catch (SQLException e) {
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error al obtener columnas de la tabla '" + table + "' en la base de datos '"
                    + database + "': " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @PostMapping("/create")
    public ResponseEntity<?> createDataWarehouse(@RequestBody DataWarehouseRequest request) {
        try {
            // Log para debuggear la estructura recibida
            System.out.println("=== DEBUG: Estructura recibida ===");
            System.out.println("Name: " + request.getName());
            System.out.println("TableName: " + request.getTableName());
            System.out.println("SelectedTables: "
                    + (request.getSelectedTables() != null ? request.getSelectedTables().size() : "null"));
            System.out.println("SelectedColumns: "
                    + (request.getSelectedColumns() != null ? request.getSelectedColumns().size() : "null"));
            System.out.println("Relationships: "
                    + (request.getRelationships() != null ? request.getRelationships().size() : "null"));

            if (request.getSelectedColumns() != null) {
                for (DataWarehouseRequest.SelectedColumn col : request.getSelectedColumns()) {
                    System.out.println("Column: " + col.getDatabase() + "." + col.getTable() + "." + col.getColumn()
                            + " -> " + col.getAlias());
                }
            }

            Map<String, Object> result = baseDatosService.crearDataWarehouse(request);

            // La nueva respuesta ya no tiene campo "success", sino que se determina por la
            // presencia de "warehouseName"
            if (result.get("warehouseName") != null) {
                return ResponseEntity.ok(result);
            } else {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(result);
            }
        } catch (Exception e) {
            e.printStackTrace(); // Imprimir el stack trace completo
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("warehouseName", null);
            errorResponse.put("message", "Error al crear Data Warehouse: " + e.getMessage());
            errorResponse.put("sql", null);
            errorResponse.put("details", null);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @GetMapping("/list")
    public ResponseEntity<?> listDataWarehouses() {
        try {
            List<WarehouseListInfo> warehouses = baseDatosService.listarDataWarehousesConTablas();
            Map<String, Object> response = new HashMap<>();
            response.put("warehouses", warehouses);
            return ResponseEntity.ok(response);
        } catch (SQLException e) {
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error al listar data warehouses: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @DeleteMapping("/delete")
    public ResponseEntity<?> deleteDataWarehouse(@RequestBody Map<String, String> request) {
        try {
            String warehouseName = request.get("name");
            if (warehouseName == null || warehouseName.trim().isEmpty()) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("error", "El nombre del Data Warehouse es requerido");
                errorResponse.put("code", "MISSING_NAME");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
            }

            Map<String, Object> result = baseDatosService.eliminarDataWarehouse(warehouseName.trim());
            if ((Boolean) result.get("success")) {
                Map<String, Object> successResponse = new HashMap<>();
                successResponse.put("message", "Data Warehouse eliminado exitosamente");
                successResponse.put("deletedDatabase", warehouseName.trim());
                return ResponseEntity.ok(successResponse);
            } else {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(result);
            }
        } catch (Exception e) {
            Map<String, Object> errorResponse = new HashMap<>();

            // Detectar si es un error de base de datos en uso
            if (e.getMessage().toLowerCase().contains("database is being used") ||
                    e.getMessage().toLowerCase().contains("cannot drop") ||
                    e.getMessage().toLowerCase().contains("in use")) {

                errorResponse.put("error", "No se puede eliminar la base de datos porque está en uso");
                errorResponse.put("code", "DATABASE_IN_USE");
                errorResponse.put("suggestion", "Cierra todas las conexiones activas e intenta nuevamente");
            } else {
                errorResponse.put("error", "Error al eliminar Data Warehouse: " + e.getMessage());
                errorResponse.put("code", "DELETION_ERROR");
            }

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @DeleteMapping("/delete/{warehouseName}")
    public ResponseEntity<?> deleteDataWarehouseByPath(@PathVariable String warehouseName) {
        try {
            if (warehouseName == null || warehouseName.trim().isEmpty()) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("error", "El nombre del Data Warehouse es requerido");
                errorResponse.put("code", "MISSING_NAME");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
            }

            Map<String, Object> result = baseDatosService.eliminarDataWarehouse(warehouseName.trim());
            if ((Boolean) result.get("success")) {
                Map<String, Object> successResponse = new HashMap<>();
                successResponse.put("message", "Data Warehouse eliminado exitosamente");
                successResponse.put("deletedDatabase", warehouseName.trim());
                return ResponseEntity.ok(successResponse);
            } else {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(result);
            }
        } catch (Exception e) {
            Map<String, Object> errorResponse = new HashMap<>();

            // Detectar si es un error de base de datos en uso
            if (e.getMessage().toLowerCase().contains("database is being used") ||
                    e.getMessage().toLowerCase().contains("cannot drop") ||
                    e.getMessage().toLowerCase().contains("in use")) {

                errorResponse.put("error", "No se puede eliminar la base de datos porque está en uso");
                errorResponse.put("code", "DATABASE_IN_USE");
                errorResponse.put("suggestion", "Cierra todas las conexiones activas e intenta nuevamente");
            } else {
                errorResponse.put("error", "Error al eliminar Data Warehouse: " + e.getMessage());
                errorResponse.put("code", "DELETION_ERROR");
            }

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @PostMapping("/test-structure")
    public ResponseEntity<?> testStructure(@RequestBody Map<String, Object> rawRequest) {
        try {
            System.out.println("=== DEBUG: Raw JSON recibido ===");
            System.out.println(rawRequest.toString());

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("receivedKeys", rawRequest.keySet());
            response.put("message", "Estructura recibida correctamente");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Error al procesar estructura: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @PostMapping("/query")
    public ResponseEntity<?> queryDataWarehouse(@RequestBody WarehouseQueryRequest request) {
        try {
            Map<String, Object> resultado = baseDatosService.ejecutarConsultaWarehouse(request);
            return ResponseEntity.ok(resultado);
        } catch (Exception e) {
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error al ejecutar consulta en Data Warehouse: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @GetMapping("/columns")
    public ResponseEntity<?> getWarehouseColumns(@RequestParam(name = "name", required = true) String warehouseName) {
        try {
            List<Map<String, Object>> columns = baseDatosService.obtenerColumnasDeWarehouse(warehouseName);
            Map<String, Object> response = new HashMap<>();
            response.put("columns", columns);
            return ResponseEntity.ok(response);
        } catch (SQLException e) {
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("error",
                    "Error al obtener columnas del Data Warehouse '" + warehouseName + "': " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
}
