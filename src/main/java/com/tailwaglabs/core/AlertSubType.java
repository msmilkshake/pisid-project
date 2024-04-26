package com.tailwaglabs.core;

public enum AlertSubType {
    TEMPERATURE_SENSOR_ABSENCE(1),
    TEMPERATURE_VARIATION(2), // It is done by a trigger
    TEMPERATURE_NEAR_LIMIT_MIN(3),
    TEMPERATURE_NEAR_LIMIT_MAX(4),
    TEMPERATURE_LIMIT_REACHED_MIN(5),
    TEMPERATURE_LIMIT_REACHED_MAX(6),
    TEMPERATURE_OUTLIERS(7),
    RATS_MAX_REACHED(8),
    EXPERIMENT_DURATION(9),
    DATABASE_INACCESSIBLE(10),
    SENSOR_ERRORS(11),
    ILLEGAL_MOVEMENT(12),
    RAT_MOVEMENT_ABSENCE(13);


    private final int value;

    AlertSubType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}