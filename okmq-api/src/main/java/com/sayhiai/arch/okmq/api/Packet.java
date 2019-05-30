package com.sayhiai.arch.okmq.api;

import lombok.Data;

import java.util.UUID;

@Data
public class Packet {
    private String content;
    private String topic;
    private String identify = UUID.randomUUID().toString();


    private long timestamp = System.currentTimeMillis();
}
