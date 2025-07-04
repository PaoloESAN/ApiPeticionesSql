package com.abd.peticionsql.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class InfoVistaResponse {

    @JsonProperty("campoFiltro")
    private String campoFiltro;

    @JsonProperty("dimensionX")
    private String dimensionX;

    @JsonProperty("dimensionY")
    private String dimensionY;

    @JsonProperty("columnas")
    private List<String> columnas;

    // Constructores
    public InfoVistaResponse() {
    }

    public InfoVistaResponse(String campoFiltro, String dimensionX, String dimensionY, List<String> columnas) {
        this.campoFiltro = campoFiltro;
        this.dimensionX = dimensionX;
        this.dimensionY = dimensionY;
        this.columnas = columnas;
    }

    // Getters y Setters
    public String getCampoFiltro() {
        return campoFiltro;
    }

    public void setCampoFiltro(String campoFiltro) {
        this.campoFiltro = campoFiltro;
    }

    public String getDimensionX() {
        return dimensionX;
    }

    public void setDimensionX(String dimensionX) {
        this.dimensionX = dimensionX;
    }

    public String getDimensionY() {
        return dimensionY;
    }

    public void setDimensionY(String dimensionY) {
        this.dimensionY = dimensionY;
    }

    public List<String> getColumnas() {
        return columnas;
    }

    public void setColumnas(List<String> columnas) {
        this.columnas = columnas;
    }
}
