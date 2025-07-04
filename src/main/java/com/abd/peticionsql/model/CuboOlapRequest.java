package com.abd.peticionsql.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CuboOlapRequest {

    @JsonProperty("baseDatos")
    private String baseDatos;

    @JsonProperty("tabla")
    private String tabla;

    @JsonProperty("dimensionX")
    private String dimensionX;

    @JsonProperty("dimensionY")
    private String dimensionY;

    @JsonProperty("campoValores")
    private String campoValores;

    @JsonProperty("tipoAgregacion")
    private String tipoAgregacion;

    @JsonProperty("campoFiltro")
    private String campoFiltro;

    @JsonProperty("valorFiltro")
    private String valorFiltro;

    // Constructores
    public CuboOlapRequest() {
    }

    public CuboOlapRequest(String baseDatos, String tabla, String dimensionX, String dimensionY,
            String campoValores, String tipoAgregacion, String campoFiltro, String valorFiltro) {
        this.baseDatos = baseDatos;
        this.tabla = tabla;
        this.dimensionX = dimensionX;
        this.dimensionY = dimensionY;
        this.campoValores = campoValores;
        this.tipoAgregacion = tipoAgregacion;
        this.campoFiltro = campoFiltro;
        this.valorFiltro = valorFiltro;
    }

    // Getters y Setters
    public String getBaseDatos() {
        return baseDatos;
    }

    public void setBaseDatos(String baseDatos) {
        this.baseDatos = baseDatos;
    }

    public String getTabla() {
        return tabla;
    }

    public void setTabla(String tabla) {
        this.tabla = tabla;
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

    public String getCampoValores() {
        return campoValores;
    }

    public void setCampoValores(String campoValores) {
        this.campoValores = campoValores;
    }

    public String getTipoAgregacion() {
        return tipoAgregacion;
    }

    public void setTipoAgregacion(String tipoAgregacion) {
        this.tipoAgregacion = tipoAgregacion;
    }

    public String getCampoFiltro() {
        return campoFiltro;
    }

    public void setCampoFiltro(String campoFiltro) {
        this.campoFiltro = campoFiltro;
    }

    public String getValorFiltro() {
        return valorFiltro;
    }

    public void setValorFiltro(String valorFiltro) {
        this.valorFiltro = valorFiltro;
    }
}
