package com.shakti.flinkdemo.model;

import com.shakti.flinkdemo.common.Times;

public record AuthEvent(String userId, String type, long eventTime) {

    public boolean isFail() {
        return "FAIL".equals(type);
    }

    public boolean isSuccess() {
        return "SUCCESS".equals(type);
    }

    public static AuthEvent parse(String line) {
        String[] f = line.split(",");
        return new AuthEvent(f[0].trim(), f[1].trim(), Times.parse(f[2]));
    }
}
