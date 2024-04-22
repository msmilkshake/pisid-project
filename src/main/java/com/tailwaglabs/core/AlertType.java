package com.tailwaglabs.core;

public enum AlertType {

    PERIGO(1),
    AVISO(2),
    INFORMATIVO(3);

    private final int value;

    AlertType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
