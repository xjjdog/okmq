package com.sayhiai.arch.okmq.api.producer;

import com.sayhiai.arch.okmq.api.Packet;

public interface Callback {
    <T> void callback(Packet packet, T orgiMeta, Exception exception);
}
