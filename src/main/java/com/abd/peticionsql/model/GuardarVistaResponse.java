package com.abd.peticionsql.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GuardarVistaResponse {

    @JsonProperty("success")
    private boolean success;

    @JsonProperty("message")
    private String message;

    @JsonProperty("nombreVista")
    private String nombreVista;

    // Constructores
    public GuardarVistaResponse() {
    }

    public GuardarVistaResponse(boolean success, String message, String nombreVista) {
        this.success = success;
        this.message = message;
        this.nombreVista = nombreVista;
    }

    // Getters y Setters
    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getNombreVista() {
        return nombreVista;
    }

    public void setNombreVista(String nombreVista) {
        this.nombreVista = nombreVista;
    }
}
