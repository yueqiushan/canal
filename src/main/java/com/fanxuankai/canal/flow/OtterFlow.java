package com.fanxuankai.canal.flow;

/**
 * @author fanxuankai
 */
public class OtterFlow {
    /**
     * 创建流化的 Otter 实例
     *
     * @param connectConfig    canal 连接配置
     * @param subscriberConfig 订阅配置
     * @return Otter
     */
    public static Otter withFlow(ConnectConfig connectConfig, HandleSubscriber.Config subscriberConfig) {
        Otter otter = new SimpleOtter(connectConfig);
        String subscriberName = subscriberConfig.getSubscriberName();
        FilterAndConvertProcessor filterAndConvertProcessor = new FilterAndConvertProcessor(subscriberName);
        HandleSubscriber handleSubscriber = new HandleSubscriber(subscriberConfig);
        ConfirmSubscriber confirmSubscriber = new ConfirmSubscriber(subscriberName);
        otter.subscribe(filterAndConvertProcessor);
        filterAndConvertProcessor.subscribe(handleSubscriber);
        handleSubscriber.subscribe(confirmSubscriber);
        return otter;
    }
}
