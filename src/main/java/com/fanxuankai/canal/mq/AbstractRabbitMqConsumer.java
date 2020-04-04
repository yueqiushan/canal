package com.fanxuankai.canal.mq;

import org.springframework.amqp.core.AmqpTemplate;

/**
 * @author fanxuankai
 */
public abstract class AbstractRabbitMqConsumer extends AbstractMqConsumer {

    protected AmqpTemplate amqpTemplate;

    public AbstractRabbitMqConsumer(AmqpTemplate amqpTemplate) {
        this.amqpTemplate = amqpTemplate;
    }

    @Override
    public void consume(MessageInfo messageInfo) {
        messageInfo.getMessages().forEach(s -> amqpTemplate.convertAndSend(messageInfo.getRoutingKey(), s));
    }

}
