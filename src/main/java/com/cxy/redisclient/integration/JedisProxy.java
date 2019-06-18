package com.cxy.redisclient.integration;

import com.cxy.redisclient.domain.Server;
import java.lang.reflect.Method;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.InvocationHandler;

/**
 * @author kongyong
 * @date 2019/6/18
 */
public class JedisProxy implements InvocationHandler {

    private Server server;
    private Object target;

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // 集群模式不支持select命令
        if (server != null && server.isJedisClusterType() && SELECT.equals(method.getName())) {
            return null;
        }
        return method.invoke(target, args);
    }

    private static final String SELECT = "select";

    public Object getInstance(Object target) {
        this.target = target;
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(this.target.getClass());
        enhancer.setCallback(this);
        return enhancer.create();
    }

    public Server getServer() {
        return server;
    }

    public void setServer(Server server) {
        this.server = server;
    }
}
