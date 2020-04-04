package com.fanxuankai.canal.flow;

import com.fanxuankai.canal.wrapper.ContextWrapper;
import com.fanxuankai.canal.wrapper.MessageWrapper;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;

/**
 * @author fanxuankai
 */
@Slf4j
public class HandleSubscriber extends SubmissionPublisher<ContextWrapper> implements Flow.Processor<ContextWrapper,
        ContextWrapper> {
    private Config config;
    private Flow.Subscription subscription;

    public HandleSubscriber(Config config) {
        this.config = config;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(ContextWrapper item) {
        log.info("{} Handle batchId: {}", config.name, item.getMessageWrapper().getBatchId());
        if (!config.skip) {
            config.handler.handle(item.getMessageWrapper());
        }
        submit(item);
        subscription.request(1);
    }

    @Override
    public void onError(Throwable throwable) {
        log.error(String.format("%s %s", config.getName(), throwable.getLocalizedMessage()), throwable);
    }

    @Override
    public void onComplete() {
        log.info("{} Done", config.name);
    }

    @Data
    @Builder
    public static class Config {
        private Handler<MessageWrapper> handler;
        private String name;
        private boolean skip;
    }
}
