package com.abd.peticionsql.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class InfoVistaRequest {

    @JsonProperty("baseDatos")
    private String baseDatos;

    @JsonProperty("vista")
    private String vista;

    // Constructores
    public InfoVistaRequest() {
    }

    public InfoVistaRequest(String baseDatos, String vista) {
        this.baseDatos = baseDatos;
        this.vista = vista;
    }

    // Getters y Setters
    public String getBaseDatos() {
        return baseDatos;
    }

    public void setBaseDatos(String baseDatos) {
        this.baseDatos = baseDatos;
    }

    public String getVista() {
        return vista;
    }

    public void setVista(String vista) {
        this.vista = vista;
    }
}
