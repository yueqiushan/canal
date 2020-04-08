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
 * Canal 链接工具类, 线程隔离
 *
 * @author fanxuankai
 */
@Slf4j
public class CanalConnectorHolder {

    /**
     * 默认重试次数
     */
    private static final int DEFAULT_RETRY_COUNT = 20;
    /**
     * 链接实例
     */
    private static ThreadLocal<CanalConnector> connectorThreadLocal = new ThreadLocal<>();

    /**
     * 创建链接
     *
     * @param connectConfig 配置文件
     */
    public static void connect(ConnectConfig connectConfig) {
        CanalConnector canalConnector = connectorThreadLocal.get();
        if (canalConnector == null) {
            CanalConfig canalConfig = App.getContext().getBean(CanalConfig.class);
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
            int retry = 0;
            boolean subscribeSuccess = false;
            // 异常后重试
            while (retry++ < DEFAULT_RETRY_COUNT) {
                try {
                    canalConnector.subscribe(connectConfig.getFilter());
                    canalConnector.rollback();
                    subscribeSuccess = true;
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
            if (subscribeSuccess) {
                CanalConnectorHolder.connectorThreadLocal.set(canalConnector);
            } else {
                throw new RuntimeException("Canal 订阅失败, 请稍后重试");
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
