package com.abd.peticionsql.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GuardarVistaRequest {

    @JsonProperty("baseDatos")
    private String baseDatos;

    @JsonProperty("nombreVista")
    private String nombreVista;

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

    // Constructores
    public GuardarVistaRequest() {
    }

    public GuardarVistaRequest(String baseDatos, String nombreVista, String tabla,
            String dimensionX, String dimensionY, String campoValores,
            String tipoAgregacion, String campoFiltro) {
        this.baseDatos = baseDatos;
        this.nombreVista = nombreVista;
        this.tabla = tabla;
        this.dimensionX = dimensionX;
        this.dimensionY = dimensionY;
        this.campoValores = campoValores;
        this.tipoAgregacion = tipoAgregacion;
        this.campoFiltro = campoFiltro;
    }

    // Getters y Setters
    public String getBaseDatos() {
        return baseDatos;
    }

    public void setBaseDatos(String baseDatos) {
        this.baseDatos = baseDatos;
    }

    public String getNombreVista() {
        return nombreVista;
    }

    public void setNombreVista(String nombreVista) {
        this.nombreVista = nombreVista;
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
}
