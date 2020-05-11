package com.simplerpc.server;
@RpcService(HelloService.class)
public class HelloServiceImpl implements HelloService{
    @Override
    public String hello(String name) {
        return "Hello! "+name;
    }
}
