package com.cxy.redisclient.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.cxy.redisclient.domain.Server;
import com.cxy.redisclient.integration.ConfigFile;
import com.cxy.redisclient.integration.server.QueryDBAmount;
import com.cxy.redisclient.integration.server.QueryServerProperties;

public class ServerService {

	public int add(Server server) {
		try {
			if (server == null) {
				return -1;
			}
			int id = Integer.parseInt(ConfigFile
					.readMaxId(ConfigFile.SERVER_MAXID)) + 1;

			ConfigFile.write(ConfigFile.NAME + id, server.getName());
			ConfigFile.write(ConfigFile.HOST + id, server.getHost());
			ConfigFile.write(ConfigFile.PORT + id, server.getPort());
			ConfigFile.write(ConfigFile.PASSWORD + id, server.getPassword());

			ConfigFile.write(ConfigFile.SERVER_MAXID, String.valueOf(id));
			if ( server.isJedisClusterType()!=null ) {
				ConfigFile.write(ConfigFile.IS_CLUSTER + id, server.isJedisClusterType().toString());
			}
			if ( server.isJedisSentinelType()!=null ) {
				ConfigFile.write(ConfigFile.IS_SENTINEL + id, server.isJedisSentinelType().toString());
			}

			return id;
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	public void delete(int id) {
		try {
			ConfigFile.delete(ConfigFile.NAME + id);
			ConfigFile.delete(ConfigFile.HOST + id);
			ConfigFile.delete(ConfigFile.PORT + id);
			ConfigFile.delete(ConfigFile.PASSWORD + id);
			ConfigFile.delete(ConfigFile.IS_SENTINEL + id);
			ConfigFile.delete(ConfigFile.IS_CLUSTER + id);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	public void update(Server server) {
		try {
			ConfigFile.write(ConfigFile.NAME + server.getId(), server.getName());
			ConfigFile.write(ConfigFile.HOST + server.getId(), server.getHost());
			ConfigFile.write(ConfigFile.PORT + server.getId(), server.getPort());
			ConfigFile.write(ConfigFile.PASSWORD + server.getId(), server.getPassword());
			if (server.isJedisSentinelType() != null) {
				ConfigFile.write(ConfigFile.IS_SENTINEL + server.getId(), server.isJedisSentinelType().toString());
			}
			if (server.isJedisClusterType() != null) {
				ConfigFile.write(ConfigFile.IS_CLUSTER + server.getId(),
						server.isJedisClusterType().toString());
			}
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	public Server listById(int id) {
		try {
			Server server = null;
			if (ConfigFile.read(ConfigFile.NAME + id) != null) {
				server = new Server(id, ConfigFile.read(ConfigFile.NAME + id),
						ConfigFile.read(ConfigFile.HOST + id),
						ConfigFile.read(ConfigFile.PORT + id),
						ConfigFile.read(ConfigFile.PASSWORD + id));
				server.setJedisClusterType(Boolean.parseBoolean(ConfigFile.read(ConfigFile.IS_CLUSTER + id)));
				server.setJedisSentinelType(Boolean.parseBoolean(ConfigFile.read(ConfigFile.IS_SENTINEL + id)));
			}
			return server;
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	public List<Server> listAll() {
		try {
			int amount = Integer.parseInt(ConfigFile
					.readMaxId(ConfigFile.SERVER_MAXID));
			List<Server> servers = new ArrayList<Server>();
			for (int i = 1; i <= amount; i++) {
				Server server = listById(i);
				if (server != null)
					servers.add(listById(i));
			}

			return servers;
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	public int listDBs(int id) {
		QueryDBAmount command = new QueryDBAmount(id);
		command.execute();
		return command.getDbAmount();
	}

	public int listDBs(Server server) throws IOException {
		return listDBs(server.getId());
	}
	
	public Map<String, String[]> listInfo(int id) {
		QueryServerProperties command = new QueryServerProperties(id);
		command.execute();
		return command.getServerInfo();
	}
}
