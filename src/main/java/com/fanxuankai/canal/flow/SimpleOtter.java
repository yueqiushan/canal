package com.fanxuankai.canal.flow;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.fanxuankai.canal.util.ThreadPoolService;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;

/**
 * @author fanxuankai
 */
@Slf4j
public class SimpleOtter implements Otter {
    private volatile boolean running;
    private ConnectConfig connectConfig;
    private SubmissionPublisher<Context> publisher;

    public SimpleOtter(ConnectConfig connectConfig) {
        this.connectConfig = connectConfig;
        this.publisher = new SubmissionPublisher<>();
    }

    @Override
    public void subscribe(Flow.Subscriber<Context> subscriber) {
        publisher.subscribe(subscriber);
    }

    @Override
    public void stop() {
        this.running = false;
        publisher.close();
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        // CanalConnector 传给 subscriber 消费后再提交
        String subscriberName = connectConfig.getSubscriberName();
        ThreadPoolService.getInstance().execute(() -> {
            try {
                CanalConnectorHolder.connect(connectConfig);
                log.info("{} 启动消费", subscriberName);
                running = true;
                while (running) {
                    try {
                        // 获取指定数量的数据
                        CanalConnector canalConnector = CanalConnectorHolder.get();
                        Message message = canalConnector.getWithoutAck(connectConfig.getCanalConfig().getBatchSize());
                        long batchId = message.getId();
                        if (batchId != -1) {
                            log.info("{} Get batchId: {}", connectConfig.getSubscriberName(), batchId);
                            publisher.submit(new Context(canalConnector, message));
                        }
                    } catch (CanalClientException e) {
                        log.error("停止消费" + e.getLocalizedMessage(), e);
                        CanalConnectorHolder.reconnect(connectConfig);
                        log.info("{} 启动消费", subscriberName);
                    }
                    try {
                        TimeUnit.MILLISECONDS.sleep(connectConfig.getCanalConfig().getIntervalMillis());
                    } catch (InterruptedException e) {
                        log.error(e.getLocalizedMessage(), e);
                    }
                }
            } finally {
                CanalConnectorHolder.disconnect();
                log.info("{} 停止消费", subscriberName);
            }
        });
    }

}
