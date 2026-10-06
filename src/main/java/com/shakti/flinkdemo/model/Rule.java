package com.shakti.flinkdemo.model;

/** Fraud rule: AMOUNT_OVER (value = threshold) or MERCHANT_BLOCKED (value = merchant name). */
public record Rule(String ruleId, String type, String value) {

    public boolean matches(Txn txn) {
        return switch (type) {
            case "AMOUNT_OVER" -> txn.amount() > Double.parseDouble(value);
            case "MERCHANT_BLOCKED" -> txn.merchant().equalsIgnoreCase(value);
            default -> false;
        };
    }

    public static Rule parse(String line) {
        String[] f = line.split(",");
        return new Rule(f[0].trim(), f[1].trim(), f[2].trim());
    }
}
