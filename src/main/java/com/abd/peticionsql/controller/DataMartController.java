package com.abd.peticionsql.controller;

import com.abd.peticionsql.model.DataMartRequest;
import com.abd.peticionsql.services.BaseDatosService;
import java.util.Map;
import java.util.HashMap;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/datamart")
public class DataMartController {

    private final BaseDatosService baseDatosService;

    public DataMartController(BaseDatosService baseDatosService) {
        this.baseDatosService = baseDatosService;
    }

    @PostMapping("/create")
    public ResponseEntity<?> createDataMart(@RequestBody DataMartRequest request) {
        try {
            // Log para debuggear la estructura recibida
            System.out.println("=== DEBUG: Data Mart Request ===");
            System.out.println("Name: " + request.getName());
            System.out.println("Source Warehouse: " + request.getSourceWarehouse());
            System.out.println("Selected Columns: " +
                    (request.getSelectedColumns() != null ? request.getSelectedColumns().size() : "null"));

            if (request.getSelectedColumns() != null) {
                for (DataMartRequest.SelectedColumn col : request.getSelectedColumns()) {
                    System.out.println("Column: " + col.getName() + " (" + col.getType() + ") -> " + col.getAlias());
                }
            }

            Map<String, Object> result = baseDatosService.crearDataMart(request);

            if ((Boolean) result.get("success")) {
                return ResponseEntity.ok(result);
            } else {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(result);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Error al crear Data Mart: " + e.getMessage());
            errorResponse.put("dataMartName", null);
            errorResponse.put("sql", null);
            errorResponse.put("details", null);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
}
