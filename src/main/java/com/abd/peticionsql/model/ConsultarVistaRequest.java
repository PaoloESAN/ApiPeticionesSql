package com.abd.peticionsql.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ConsultarVistaRequest {

    @JsonProperty("baseDatos")
    private String baseDatos;

    @JsonProperty("vista")
    private String vista;

    @JsonProperty("valorFiltro")
    private String valorFiltro;

    // Constructores
    public ConsultarVistaRequest() {
    }

    public ConsultarVistaRequest(String baseDatos, String vista, String valorFiltro) {
        this.baseDatos = baseDatos;
        this.vista = vista;
        this.valorFiltro = valorFiltro;
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

    public String getValorFiltro() {
        return valorFiltro;
    }

    public void setValorFiltro(String valorFiltro) {
        this.valorFiltro = valorFiltro;
    }
}
