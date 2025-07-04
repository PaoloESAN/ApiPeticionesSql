package com.abd.peticionsql.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public class CuboOlapResponse {

    @JsonProperty("columnas")
    private List<String> columnas;

    @JsonProperty("filas")
    private List<Map<String, Object>> filas;

    @JsonProperty("dimensionX")
    private String dimensionX;

    // Constructores
    public CuboOlapResponse() {
    }

    public CuboOlapResponse(List<String> columnas, List<Map<String, Object>> filas, String dimensionX) {
        this.columnas = columnas;
        this.filas = filas;
        this.dimensionX = dimensionX;
    }

    // Getters y Setters
    public List<String> getColumnas() {
        return columnas;
    }

    public void setColumnas(List<String> columnas) {
        this.columnas = columnas;
    }

    public List<Map<String, Object>> getFilas() {
        return filas;
    }

    public void setFilas(List<Map<String, Object>> filas) {
        this.filas = filas;
    }

    public String getDimensionX() {
        return dimensionX;
    }

    public void setDimensionX(String dimensionX) {
        this.dimensionX = dimensionX;
    }
}
