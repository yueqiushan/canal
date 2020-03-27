package com.fanxuankai.canal.flow;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import lombok.Getter;

/**
 * @author fanxuankai
 */
public class Context {
    private CanalConnector canalConnector;
    @Getter
    private Message message;

    public Context(CanalConnector canalConnector, Message message) {
        this.canalConnector = canalConnector;
        this.message = message;
    }

    public void ack() {
        canalConnector.ack(message.getId());
    }

    public void rollback() {
        canalConnector.rollback(message.getId());
    }
}
