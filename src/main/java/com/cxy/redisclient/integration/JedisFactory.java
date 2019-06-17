package com.cxy.redisclient.integration;

import com.cxy.redisclient.domain.Server;
import com.cxy.redisclient.service.ServerService;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.JedisSlotBasedConnectionHandler;

/**
 * @author kongyong
 * @date 2019/6/17
 */
public class JedisFactory {

    private static final String COMMA = ",";
    private static int timeout = ConfigFile.getT1();
    private static ServerService service = new ServerService();
    private static Map<Integer, JedisSentinelPool> jedisSentinelPoolCache = new ConcurrentHashMap<>();
    private static Map<Integer, JedisSlotBasedConnectionHandler> jedisClusterConnectionHandlerCache = new ConcurrentHashMap<>();

    public static Jedis getJedis(int serverId){
        Server server = service.listById(serverId);
        Jedis jedis = null;
        if (server.isJedisSentinelType()) {
            jedis = getJedisFromSentinelPool(server);
        }
        if (server.isJedisClusterType()) {
            jedis = getJedisFromClusterConnectionHandler(server);
        }
        if (jedis == null) {
            jedis = createJedis(server);
        }
        if(server.getPassword() != null && server.getPassword().length() > 0) {
            jedis.auth(server.getPassword());
        }
        return jedis;
    }

    private static Jedis createJedis(Server server){
        return new Jedis(server.getHost(), Integer.parseInt(server.getPort()), timeout);
    }

    private static Jedis getJedisFromSentinelPool(Server server){
        if (jedisSentinelPoolCache.get(server.getId()) == null) {
            Set<HostAndPort> hostAndPorts = getHostAndPorts(server);
            Set<String> sentinels = new HashSet<>(hostAndPorts.size());
            for (HostAndPort hostAndPort : hostAndPorts) {
                sentinels.add(hostAndPort.toString());
            }
            jedisSentinelPoolCache.put(server.getId(), new JedisSentinelPool(server.getName(), sentinels, new GenericObjectPoolConfig(), timeout, server.getPassword()));
        }
        return jedisSentinelPoolCache.get(server.getId()).getResource();
    }

    private static Jedis getJedisFromClusterConnectionHandler(Server server){
        if (jedisClusterConnectionHandlerCache.get(server.getId()) == null) {
            jedisClusterConnectionHandlerCache.put(server.getId(), new JedisSlotBasedConnectionHandler(getHostAndPorts(server), new GenericObjectPoolConfig(), timeout));
        }
        return jedisClusterConnectionHandlerCache.get(server.getId()).getConnection();
    }

    private static Set<HostAndPort> getHostAndPorts(Server server) {
        String[] hosts = server.getHost().split(COMMA);
        Set<HostAndPort> hostAndPorts = new HashSet<>(hosts.length);
        if (hosts.length > 0) {
            String[] ports = server.getPort().split(COMMA);
            boolean isSameLength = true;
            if (ports.length != hosts.length) {
                isSameLength = false;
            }
            String port = ports[0];
            for (int i = 0; i<hosts.length; i++) {
                String host = hosts[i];
                if (isSameLength) {
                    port = ports[i];
                }
                HostAndPort hostAndPort = new HostAndPort(host, Integer.parseInt(port));
                hostAndPorts.add(hostAndPort);
            }
        }
        return hostAndPorts;
    }

}
