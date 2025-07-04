package com.abd.peticionsql.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class ValoresFiltroResponse {

    @JsonProperty("valores")
    private List<String> valores;

    // Constructores
    public ValoresFiltroResponse() {
    }

    public ValoresFiltroResponse(List<String> valores) {
        this.valores = valores;
    }

    // Getters y Setters
    public List<String> getValores() {
        return valores;
    }

    public void setValores(List<String> valores) {
        this.valores = valores;
    }
}
