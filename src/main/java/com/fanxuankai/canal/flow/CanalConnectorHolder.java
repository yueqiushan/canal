package com.fanxuankai.canal.flow;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.fanxuankai.canal.config.CanalConfig;
import com.fanxuankai.canal.util.App;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Canal 连接工具类, 线程隔离
 *
 * @author fanxuankai
 */
@Slf4j
public class CanalConnectorHolder {

    /**
     * 连接实例
     */
    private static ThreadLocal<CanalConnector> connectorThreadLocal = new ThreadLocal<>();

    /**
     * 默认重试次数
     */
    private static final int DEFAULT_RETRY_COUNT = 20;

    /**
     * 创建连接
     *
     * @param connectConfig 配置文件
     */
    public static void connect(ConnectConfig connectConfig) {
        CanalConnector canalConnector = connectorThreadLocal.get();
        if (canalConnector == null) {
            CanalConfig canalConfig = App.getContext().getBean(CanalConfig.class);
            int retry = 0;
            // 异常后重试
            while (retry++ < DEFAULT_RETRY_COUNT) {
                try {
                    String instance = connectConfig.getInstance();
                    if (canalConfig.getCluster() != null && !StringUtils.isEmpty(canalConfig.getCluster().getNodes())) {
                        canalConnector = CanalConnectors.newClusterConnector(canalConfig.getCluster().getNodes(),
                                instance, canalConfig.getUsername(), canalConfig.getPassword());
                    } else {
                        canalConnector =
                                CanalConnectors.newSingleConnector(
                                        new InetSocketAddress(canalConfig.getSingleNode().getHostname(),
                                                canalConfig.getSingleNode().getPort()), instance,
                                        canalConfig.getUsername(),
                                        canalConfig.getPassword());
                    }
                    canalConnector.connect();
                    canalConnector.subscribe(connectConfig.getFilter());
                    canalConnector.rollback();
                    CanalConnectorHolder.connectorThreadLocal.set(canalConnector);
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
