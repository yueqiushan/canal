package com.fanxuankai.canal.mq;

import com.xxl.mq.client.message.XxlMqMessage;
import com.xxl.mq.client.producer.XxlMqProducer;

/**
 * @author fanxuankai
 */
public abstract class AbstractXxlMqConsumer extends AbstractMqConsumer {

    @Override
    public void consume(MessageInfo messageInfo) {
        messageInfo.getMessages().forEach(s -> XxlMqProducer.produce(new XxlMqMessage(messageInfo.getRoutingKey(), s)));
    }

}
