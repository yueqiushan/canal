package com.fanxuankai.canal.flow;

/**
 * @author fanxuankai
 */
public class OtterFlow {
    /**
     * 创建流化的 Otter 实例
     *
     * @param connectConfig          canal 链接配置
     * @param handleSubscriberConfig 处理订阅者配置
     * @return Otter
     */
    public static Otter withFlow(ConnectConfig connectConfig, HandleSubscriber.Config handleSubscriberConfig) {
        Otter otter = new SimpleOtter(connectConfig);
        String subscriberName = handleSubscriberConfig.getName();
        ConvertProcessor convertProcessor = new ConvertProcessor(subscriberName);
        HandleSubscriber handleSubscriber = new HandleSubscriber(handleSubscriberConfig);
        ConfirmSubscriber confirmSubscriber = new ConfirmSubscriber(subscriberName);
        otter.subscribe(convertProcessor);
        convertProcessor.subscribe(handleSubscriber);
        handleSubscriber.subscribe(confirmSubscriber);
        return otter;
    }
}
