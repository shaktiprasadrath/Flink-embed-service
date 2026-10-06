package com.shakti.flinkdemo.model;

import com.shakti.flinkdemo.common.Times;

public record SensorReading(String sensorId, double temperature, long readingTime) {

    public static SensorReading parse(String line) {
        String[] f = line.split(",");
        return new SensorReading(f[0].trim(), Double.parseDouble(f[1].trim()), Times.parse(f[2]));
    }
}
