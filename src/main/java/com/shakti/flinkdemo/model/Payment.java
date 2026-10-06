package com.shakti.flinkdemo.model;

import com.shakti.flinkdemo.common.Times;

public record Payment(String paymentId, String orderId, double amount, long paymentTime) {

    public static Payment parse(String line) {
        String[] f = line.split(",");
        return new Payment(f[0].trim(), f[1].trim(), Double.parseDouble(f[2].trim()), Times.parse(f[3]));
    }
}
