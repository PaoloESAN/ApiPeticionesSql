package com.abd.peticionsql.model;

public class ColumnaInfo {
    private String nombre;
    private String tipo;
    private boolean esNulo;
    private boolean esPrimaria;
    private boolean esForanea;
    private String tablaReferencia;
    private String columnaReferencia;

    public ColumnaInfo() {
    }

    public ColumnaInfo(String nombre, String tipo, boolean esNulo, boolean esPrimaria, boolean esForanea,
            String tablaReferencia, String columnaReferencia) {
        this.nombre = nombre;
        this.tipo = tipo;
        this.esNulo = esNulo;
        this.esPrimaria = esPrimaria;
        this.esForanea = esForanea;
        this.tablaReferencia = tablaReferencia;
        this.columnaReferencia = columnaReferencia;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public String getTipo() {
        return tipo;
    }

    public void setTipo(String tipo) {
        this.tipo = tipo;
    }

    public boolean isEsNulo() {
        return esNulo;
    }

    public void setEsNulo(boolean esNulo) {
        this.esNulo = esNulo;
    }

    public boolean isEsPrimaria() {
        return esPrimaria;
    }

    public void setEsPrimaria(boolean esPrimaria) {
        this.esPrimaria = esPrimaria;
    }

    public boolean isEsForanea() {
        return esForanea;
    }

    public void setEsForanea(boolean esForanea) {
        this.esForanea = esForanea;
    }

    public String getTablaReferencia() {
        return tablaReferencia;
    }

    public void setTablaReferencia(String tablaReferencia) {
        this.tablaReferencia = tablaReferencia;
    }

    public String getColumnaReferencia() {
        return columnaReferencia;
    }

    public void setColumnaReferencia(String columnaReferencia) {
        this.columnaReferencia = columnaReferencia;
    }
}
