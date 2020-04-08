package com.fanxuankai.canal.flow;

import com.fanxuankai.canal.wrapper.ContextWrapper;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;

/**
 * 消息转换订阅者
 *
 * @author fanxuankai
 */
@Slf4j
public class ConvertProcessor extends SubmissionPublisher<ContextWrapper>
        implements Flow.Processor<Context, ContextWrapper> {

    private Flow.Subscription subscription;
    private String name;

    public ConvertProcessor(String name) {
        this.name = name;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(Context item) {
        if (!item.getMessage().getEntries().isEmpty()) {
            log.info("{} Filter&Convert batchId: {}", name, item.getMessage().getId());
        }
        submit(new ContextWrapper(item));
        subscription.request(1);
    }

    @Override
    public void onError(Throwable throwable) {
        log.error(String.format("%s %s", name, throwable.getLocalizedMessage()), throwable);
    }

    @Override
    public void onComplete() {
        log.info("{} Done", name);
    }
}
