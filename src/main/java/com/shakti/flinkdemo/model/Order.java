package com.shakti.flinkdemo.model;

import com.shakti.flinkdemo.common.Times;

/** One line of orders.csv. Java records are handled by Flink's POJO serializer (no Kryo). */
public record Order(String orderId, String customerId, String product, int quantity, double price,
                    String currency, long orderTime) {

    public double amount() {
        return quantity * price;
    }

    /** Parses a CSV line; throws IllegalArgumentException for malformed lines. */
    public static Order parse(String line) {
        String[] f = line.split(",", -1);
        if (f.length != 7) {
            throw new IllegalArgumentException("expected 7 fields but got " + f.length);
        }
        try {
            return new Order(f[0].trim(), f[1].trim(), f[2].trim(), Integer.parseInt(f[3].trim()),
                    Double.parseDouble(f[4].trim()), f[5].trim(), Times.parse(f[6]));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("cannot parse fields: " + e.getMessage(), e);
        }
    }
}
