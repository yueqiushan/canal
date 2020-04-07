package com.fanxuankai.canal.mq;

import com.fanxuankai.canal.util.App;
import org.springframework.amqp.core.AmqpTemplate;

/**
 * RabbitMQ 抽象消费者
 *
 * @author fanxuankai
 */
public abstract class AbstractRabbitMqConsumer extends AbstractMqConsumer {

    protected AmqpTemplate amqpTemplate;

    public AbstractRabbitMqConsumer() {
        this.amqpTemplate = App.getContext().getBean(AmqpTemplate.class);
    }

    @Override
    public void consume(MessageInfo messageInfo) {
        messageInfo.getMessages().forEach(s -> amqpTemplate.convertAndSend(messageInfo.getRoutingKey(), s));
    }

}
