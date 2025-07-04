package com.abd.peticionsql.controller;

import com.abd.peticionsql.model.*;
import com.abd.peticionsql.services.CuboOlapService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/cubo-olap")
public class CuboOlapController {

    private final CuboOlapService cuboOlapService;

    public CuboOlapController(CuboOlapService cuboOlapService) {
        this.cuboOlapService = cuboOlapService;
    }

    /**
     * Ejecutar Cubo OLAP
     * POST /api/cubo-olap/ejecutar
     */
    @PostMapping("/ejecutar")
    public ResponseEntity<?> ejecutarCuboOlap(@RequestBody CuboOlapRequest request) {
        try {
            // Validaciones básicas
            if (request.getBaseDatos() == null || request.getBaseDatos().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'baseDatos' es requerido"));
            }
            if (request.getTabla() == null || request.getTabla().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'tabla' es requerido"));
            }
            if (request.getDimensionX() == null || request.getDimensionX().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'dimensionX' es requerido"));
            }
            if (request.getDimensionY() == null || request.getDimensionY().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'dimensionY' es requerido"));
            }
            if (request.getCampoValores() == null || request.getCampoValores().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'campoValores' es requerido"));
            }
            if (request.getTipoAgregacion() == null || request.getTipoAgregacion().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'tipoAgregacion' es requerido"));
            }

            // Validar tipo de agregación
            String tipoAgregacion = request.getTipoAgregacion().toUpperCase();
            if (!tipoAgregacion.matches("SUM|COUNT|AVG|MAX|MIN")) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El tipo de agregación debe ser: SUM, COUNT, AVG, MAX o MIN"));
            }
            request.setTipoAgregacion(tipoAgregacion);

            CuboOlapResponse response = cuboOlapService.ejecutarCuboOlap(request);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Error al ejecutar cubo OLAP: " + e.getMessage()));
        }
    }

    /**
     * Obtener Valores Únicos para Filtro
     * POST /api/cubo-olap/valores-filtro
     */
    @PostMapping("/valores-filtro")
    public ResponseEntity<?> obtenerValoresFiltro(@RequestBody ValoresFiltroRequest request) {
        try {
            // Validaciones básicas
            if (request.getBaseDatos() == null || request.getBaseDatos().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'baseDatos' es requerido"));
            }
            if (request.getTabla() == null || request.getTabla().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'tabla' es requerido"));
            }
            if (request.getCampo() == null || request.getCampo().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'campo' es requerido"));
            }

            ValoresFiltroResponse response = cuboOlapService.obtenerValoresFiltro(request);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Error al obtener valores de filtro: " + e.getMessage()));
        }
    }

    /**
     * Guardar Vista del Cubo OLAP
     * POST /api/cubo-olap/guardar-vista
     */
    @PostMapping("/guardar-vista")
    public ResponseEntity<?> guardarVista(@RequestBody GuardarVistaRequest request) {
        try {
            // Validaciones básicas
            if (request.getBaseDatos() == null || request.getBaseDatos().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'baseDatos' es requerido"));
            }
            if (request.getNombreVista() == null || request.getNombreVista().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'nombreVista' es requerido"));
            }
            if (request.getTabla() == null || request.getTabla().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'tabla' es requerido"));
            }
            if (request.getDimensionX() == null || request.getDimensionX().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'dimensionX' es requerido"));
            }
            if (request.getDimensionY() == null || request.getDimensionY().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'dimensionY' es requerido"));
            }
            if (request.getCampoValores() == null || request.getCampoValores().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'campoValores' es requerido"));
            }
            if (request.getTipoAgregacion() == null || request.getTipoAgregacion().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'tipoAgregacion' es requerido"));
            }

            // Validar tipo de agregación
            String tipoAgregacion = request.getTipoAgregacion().toUpperCase();
            if (!tipoAgregacion.matches("SUM|COUNT|AVG|MAX|MIN")) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El tipo de agregación debe ser: SUM, COUNT, AVG, MAX o MIN"));
            }
            request.setTipoAgregacion(tipoAgregacion);

            // Validar nombre de vista (solo caracteres alfanuméricos y guiones bajos)
            if (!request.getNombreVista().matches("^[a-zA-Z0-9_]+$")) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse(
                                "El nombre de la vista solo puede contener letras, números y guiones bajos"));
            }

            GuardarVistaResponse response = cuboOlapService.guardarVista(request);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Error al guardar vista: " + e.getMessage()));
        }
    }

    /**
     * Consultar Vista del Cubo OLAP
     * POST /api/cubo-olap/consultar-vista
     */
    @PostMapping("/consultar-vista")
    public ResponseEntity<?> consultarVista(@RequestBody ConsultarVistaRequest request) {
        try {
            // Validaciones básicas
            if (request.getBaseDatos() == null || request.getBaseDatos().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'baseDatos' es requerido"));
            }
            if (request.getVista() == null || request.getVista().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'vista' es requerido"));
            }

            CuboOlapResponse response = cuboOlapService.consultarVista(request);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Error al consultar vista: " + e.getMessage()));
        }
    }

    /**
     * Obtener Información de Vista
     * POST /api/cubo-olap/info-vista
     */
    @PostMapping("/info-vista")
    public ResponseEntity<?> obtenerInfoVista(@RequestBody InfoVistaRequest request) {
        try {
            // Validaciones básicas
            if (request.getBaseDatos() == null || request.getBaseDatos().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'baseDatos' es requerido"));
            }
            if (request.getVista() == null || request.getVista().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'vista' es requerido"));
            }

            InfoVistaResponse response = cuboOlapService.obtenerInfoVista(request);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Error al obtener información de vista: " + e.getMessage()));
        }
    }

    /**
     * Obtener Valores de Filtro para Vista
     * POST /api/cubo-olap/valores-filtro-vista
     */
    @PostMapping("/valores-filtro-vista")
    public ResponseEntity<?> obtenerValoresFiltroVista(@RequestBody ValoresFiltroVistaRequest request) {
        try {
            // Validaciones básicas
            if (request.getBaseDatos() == null || request.getBaseDatos().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'baseDatos' es requerido"));
            }
            if (request.getVista() == null || request.getVista().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'vista' es requerido"));
            }
            if (request.getCampo() == null || request.getCampo().trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("El campo 'campo' es requerido"));
            }

            ValoresFiltroResponse response = cuboOlapService.obtenerValoresFiltroVista(request);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Error al obtener valores de filtro de vista: " + e.getMessage()));
        }
    }

    /**
     * Método auxiliar para crear respuestas de error consistentes
     */
    private Map<String, String> createErrorResponse(String message) {
        Map<String, String> errorResponse = new HashMap<>();
        errorResponse.put("error", message);
        return errorResponse;
    }
}
