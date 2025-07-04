package com.abd.peticionsql.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ValoresFiltroVistaRequest {

    @JsonProperty("baseDatos")
    private String baseDatos;

    @JsonProperty("vista")
    private String vista;

    @JsonProperty("campo")
    private String campo;

    // Constructores
    public ValoresFiltroVistaRequest() {
    }

    public ValoresFiltroVistaRequest(String baseDatos, String vista, String campo) {
        this.baseDatos = baseDatos;
        this.vista = vista;
        this.campo = campo;
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

    public String getCampo() {
        return campo;
    }

    public void setCampo(String campo) {
        this.campo = campo;
    }
}
