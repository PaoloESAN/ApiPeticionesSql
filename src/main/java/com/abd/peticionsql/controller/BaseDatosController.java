package com.abd.peticionsql.controller;

import com.abd.peticionsql.model.ColumnaInfo;
import com.abd.peticionsql.services.BaseDatosService;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.sql.SQLException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class BaseDatosController {

    private final BaseDatosService baseDatosService;

    public BaseDatosController(BaseDatosService baseDatosService) {
        this.baseDatosService = baseDatosService;
    }

    @PostMapping("/crearBase")
    public ResponseEntity<Map<String, String>> crearBase(
            @RequestParam(name = "nombre", required = true) String nombre) {
        try {
            baseDatosService.crearBaseDatos(nombre);
            Map<String, String> response = new HashMap<>();
            response.put("respuesta", "Base de datos '" + nombre + "' creada correctamente.");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("respuesta", "Error al crear la base: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(errorResponse);
        }
    }

    @PostMapping("/eliminarBase")
    public ResponseEntity<Map<String, String>> eliminarBase(
            @RequestParam(name = "nombre", required = true) String nombre) {
        try {
            baseDatosService.eliminarBaseDatos(nombre);
            Map<String, String> response = new HashMap<>();
            response.put("respuesta", "Base de datos '" + nombre + "' eliminada correctamente.");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("respuesta", "Error al eliminar la base: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(errorResponse);
        }
    }

    @PostMapping("/consultaBase")
    public ResponseEntity<Map<String, String>> consultaBase(
            @RequestParam(name = "nombre", required = true) String nombre,
            @RequestParam(name = "sql", required = true) String sql) {
        try {
            String resultado = baseDatosService.comandoPersonalizado(nombre, sql);
            Map<String, String> response = new HashMap<>();
            response.put("respuesta", resultado);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("respuesta", "Error al ejecutar la consulta: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(errorResponse);
        }
    }

    @GetMapping("/consultaSelect")
    public ResponseEntity<Map<String, String>> consultaSelect(
            @RequestParam(name = "bd", required = true) String nombreBD,
            @RequestParam(name = "sql", required = true) String sql) {
        try {
            String resultado = baseDatosService.ejecutarSelect(nombreBD, sql);
            Map<String, String> response = new HashMap<>();
            response.put("respuesta", resultado);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("respuesta", "Error al ejecutar la consulta SELECT: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(errorResponse);
        }
    }

    @GetMapping("/listarBases")
    public ResponseEntity<?> listarBases() {
        try {
            List<String> databases = baseDatosService.listarBasesDatos();
            return ResponseEntity.ok(databases);
        } catch (SQLException e) {
            Map<String, String> response = new HashMap<>();
            response.put("error", "Error al listar las bases de datos: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    @GetMapping("/tablas")
    public ResponseEntity<?> listarTablas(@RequestParam(name = "nombre", required = true) String nombre) {
        try {
            List<String> tablas = baseDatosService.listarTablas(nombre);
            return ResponseEntity.ok(tablas);
        } catch (SQLException e) {
            Map<String, String> response = new HashMap<>();
            response.put("error", "Error al listar las tablas de la base de datos '" + nombre + "': " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    @GetMapping("/columnas")
    public ResponseEntity<?> listarColumnas(
            @RequestParam(name = "tabla", required = true) String nombreTabla,
            @RequestParam(name = "bd", required = true) String nombreBD) {
        try {
            List<ColumnaInfo> columnas = baseDatosService.listarColumnas(nombreBD, nombreTabla);
            return ResponseEntity.ok(columnas);
        } catch (SQLException e) {
            Map<String, String> response = new HashMap<>();
            response.put("error", "Error al listar las columnas de la tabla '" + nombreTabla + "' en la base de datos '"
                    + nombreBD + "': " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    @GetMapping("/vistas")
    public ResponseEntity<?> listarVistas(@RequestParam(name = "bd", required = true) String nombreBD) {
        try {
            List<String> vistas = baseDatosService.listarVistas(nombreBD);
            return ResponseEntity.ok(vistas);
        } catch (SQLException e) {
            Map<String, String> response = new HashMap<>();
            response.put("error",
                    "Error al listar las vistas de la base de datos '" + nombreBD + "': " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    @GetMapping(value = "/datos", produces = "text/plain;charset=UTF-8")
    public ResponseEntity<String> obtenerDatosBaseDatos(@RequestParam(name = "bd", required = true) String nombreBD) {
        try {
            String datos = baseDatosService.obtenerDatosBaseDatos(nombreBD);
            return ResponseEntity.ok(datos);
        } catch (SQLException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error al obtener los datos de la base de datos '" + nombreBD + "': " + e.getMessage());
        }
    }

    @GetMapping("/procedures")
    public ResponseEntity<?> listarStoredProcedures(@RequestParam(name = "bd", required = true) String nombreBD) {
        try {
            List<String> procedures = baseDatosService.listarStoredProcedures(nombreBD);
            return ResponseEntity.ok(procedures);
        } catch (SQLException e) {
            Map<String, String> response = new HashMap<>();
            response.put("error",
                    "Error al listar los stored procedures de la base de datos '" + nombreBD + "': " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

}