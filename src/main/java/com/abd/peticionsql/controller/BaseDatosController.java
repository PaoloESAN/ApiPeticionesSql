package com.abd.peticionsql.controller;

import com.abd.peticionsql.services.BaseDatosService;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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
    public ResponseEntity<String> crearBase(@RequestParam(name = "nombre", required = true) String nombre) {
        try {
            baseDatosService.crearBaseDatos(nombre);
            return ResponseEntity.ok("Base de datos '" + nombre + "' creada correctamente.");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error al crear la base: " + e.getMessage());
        }
    }

    @PostMapping("/eliminarBase")
    public ResponseEntity<String> eliminarBase(@RequestParam(name = "nombre", required = true) String nombre) {
        try {
            baseDatosService.eliminarBaseDatos(nombre);
            return ResponseEntity.ok("Base de datos '" + nombre + "' eliminada correctamente.");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error al eliminar la base: " + e.getMessage());
        }
    }

    @PostMapping("/consultaBase")
    public ResponseEntity<String> consultaBase(@RequestParam(name = "nombre", required = true) String nombre,
            @RequestParam(name = "sql", required = true) String sql) {
        try {
            baseDatosService.comandoPersonalizado(nombre, sql);
            return ResponseEntity.ok("Consulta ejecutada correctamente.");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error al ejecutar la consulta: " + e.getMessage());
        }
    }
}