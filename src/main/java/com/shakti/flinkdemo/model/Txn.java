package com.shakti.flinkdemo.model;

import com.shakti.flinkdemo.common.Times;

public record Txn(String txnId, String accountId, double amount, String merchant, long txnTime) {

    public static Txn parse(String line) {
        String[] f = line.split(",");
        return new Txn(f[0].trim(), f[1].trim(), Double.parseDouble(f[2].trim()), f[3].trim(), Times.parse(f[4]));
    }
}
