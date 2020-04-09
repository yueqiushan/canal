package com.fanxuankai.canal.flow;

import java.util.concurrent.SubmissionPublisher;

/**
 * Otter 并行流客户端
 *
 * @author fanxuankai
 */
public class FlowOtter extends AbstractOtter {

    private SubmissionPublisher<Context> publisher = new SubmissionPublisher<>();

    public FlowOtter(ConnectConfig connectConfig, HandleSubscriber.Config handleSubscriberConfig) {
        super(connectConfig);

        // 流转换订阅者
        ConvertProcessor convertProcessor = new ConvertProcessor(handleSubscriberConfig.getName());
        publisher.subscribe(convertProcessor);

        // 流处理订阅者
        HandleSubscriber handleSubscriber = new HandleSubscriber(handleSubscriberConfig);
        convertProcessor.subscribe(handleSubscriber);

        // 流确认订阅者
        ConfirmSubscriber confirmSubscriber = new ConfirmSubscriber(handleSubscriberConfig.getName());
        handleSubscriber.subscribe(confirmSubscriber);
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void process(Context context) {
        publisher.submit(context);
    }

    @Override
    public void stop() {
        super.stop();
        publisher.close();
    }
}
