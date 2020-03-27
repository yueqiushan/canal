package com.fanxuankai.canal.flow;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.fanxuankai.canal.config.CanalConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @author fanxuankai
 */
@Slf4j
public class CanalConnectorHolder {

    private static ThreadLocal<CanalConnector> connectorThreadLocal = new ThreadLocal<>();
    private static ThreadLocal<ConnectConfig> configThreadLocal = new ThreadLocal<>();

    public static void connect(ConnectConfig connectConfig) {
        CanalConnector canalConnector = connectorThreadLocal.get();
        if (canalConnector == null) {
            while (true) {
                try {
                    CanalConfig canalConfig = connectConfig.getCanalConfig();
                    String instance = connectConfig.getInstance();
                    if (canalConfig.getCluster() != null && !StringUtils.isEmpty(canalConfig.getCluster().getNodes())) {
                        canalConnector = CanalConnectors.newClusterConnector(canalConfig.getCluster().getNodes(),
                                instance, canalConfig.getUsername(), canalConfig.getPassword());
                    } else {
                        canalConnector =
                                CanalConnectors.newSingleConnector(new InetSocketAddress(canalConfig.getSingleNode().getHostname(),
                                                canalConfig.getSingleNode().getPort()), instance,
                                        canalConfig.getUsername(),
                                        canalConfig.getPassword());
                    }
                    canalConnector.connect();
                    canalConnector.subscribe(connectConfig.getFilter());
                    canalConnector.rollback();
                    CanalConnectorHolder.connectorThreadLocal.set(canalConnector);
                    CanalConnectorHolder.configThreadLocal.set(connectConfig);
                    break;
                } catch (CanalClientException e) {
                    log.error(e.getLocalizedMessage(), e);
                    try {
                        TimeUnit.SECONDS.sleep(2);
                    } catch (Exception ex) {
                        log.error(e.getLocalizedMessage(), ex);
                    }
                }
            }
        }
    }

    public static void reconnect(ConnectConfig connectConfig) {
        connectorThreadLocal.remove();
        configThreadLocal.remove();
        connect(connectConfig);
    }

    public static void disconnect() {
        CanalConnector canalConnector = connectorThreadLocal.get();
        if (canalConnector != null) {
            canalConnector.unsubscribe();
            canalConnector.disconnect();
            connectorThreadLocal.remove();
        }
    }

    public static CanalConnector get() {
        return connectorThreadLocal.get();
    }
}
