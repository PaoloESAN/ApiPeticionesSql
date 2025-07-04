package com.abd.peticionsql.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ValoresFiltroRequest {

    @JsonProperty("baseDatos")
    private String baseDatos;

    @JsonProperty("tabla")
    private String tabla;

    @JsonProperty("campo")
    private String campo;

    // Constructores
    public ValoresFiltroRequest() {
    }

    public ValoresFiltroRequest(String baseDatos, String tabla, String campo) {
        this.baseDatos = baseDatos;
        this.tabla = tabla;
        this.campo = campo;
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

    public String getCampo() {
        return campo;
    }

    public void setCampo(String campo) {
        this.campo = campo;
    }
}
