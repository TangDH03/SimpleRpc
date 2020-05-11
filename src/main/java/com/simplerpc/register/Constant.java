package com.simplerpc.register;

public interface Constant {
    int ZK_SESSION_TIMEOUT = 5000;
    String ZK_REGISTERY_PATH = "/registry";
    String ZK_DATA_PATH = ZK_REGISTERY_PATH+"/data";
}
