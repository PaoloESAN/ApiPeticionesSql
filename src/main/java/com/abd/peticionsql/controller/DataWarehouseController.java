package com.abd.peticionsql.controller;

import com.abd.peticionsql.model.DatabaseInfo;
import com.abd.peticionsql.model.DataWarehouseRequest;
import com.abd.peticionsql.model.TableColumnInfo;
import com.abd.peticionsql.model.WarehouseInfo;
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
            if ((Boolean) result.get("success")) {
                return ResponseEntity.ok(result);
            } else {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(result);
            }
        } catch (Exception e) {
            e.printStackTrace(); // Imprimir el stack trace completo
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Error al crear Data Warehouse: " + e.getMessage());
            errorResponse.put("warehouseName", null);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @GetMapping("/list")
    public ResponseEntity<?> listDataWarehouses() {
        try {
            List<WarehouseInfo> warehouses = baseDatosService.listarDataWarehouses();
            Map<String, Object> response = new HashMap<>();
            response.put("warehouses", warehouses);
            return ResponseEntity.ok(response);
        } catch (SQLException e) {
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error al listar data warehouses: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @DeleteMapping("/delete/{warehouse_name}")
    public ResponseEntity<?> deleteDataWarehouse(@PathVariable("warehouse_name") String warehouseName) {
        try {
            Map<String, Object> result = baseDatosService.eliminarDataWarehouse(warehouseName);
            if ((Boolean) result.get("success")) {
                return ResponseEntity.ok(result);
            } else {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(result);
            }
        } catch (Exception e) {
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Error al eliminar Data Warehouse: " + e.getMessage());
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
            String resultado = baseDatosService.ejecutarConsultaWarehouse(request);
            Map<String, String> response = new HashMap<>();
            response.put("respuesta", resultado);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("respuesta", "Error al ejecutar consulta en Data Warehouse: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
}
