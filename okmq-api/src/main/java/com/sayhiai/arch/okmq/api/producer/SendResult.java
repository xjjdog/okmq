package com.sayhiai.arch.okmq.api.producer;


import lombok.Data;

@Data
public class SendResult {
    private int code;
    private String msg;


    public static final int OK = 200;
    public static final int ERROR = 500;
}

