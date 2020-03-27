package com.fanxuankai.canal.flow;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.fanxuankai.canal.wrapper.ContextWrapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.stream.Collectors;

import static com.alibaba.otter.canal.protocol.CanalEntry.EventType.*;

/**
 * @author fanxuankai
 */
@Slf4j
public class FilterAndConvertProcessor extends SubmissionPublisher<ContextWrapper> implements Flow.Processor<Context,
        ContextWrapper> {

    /**
     * 过滤的事件类型
     */
    private static final List<CanalEntry.EventType> EVENT_TYPES = Arrays.asList(INSERT, DELETE, UPDATE, ERASE);

    private Flow.Subscription subscription;
    private String name;

    public FilterAndConvertProcessor(String name) {
        this.name = name;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(Context item) {
        log.info("{} Filter&Convert batchId: {}", name, item.getMessage().getId());
        Message message = item.getMessage();
        message.setEntries(filter(message.getEntries()));
        submit(new ContextWrapper(item));
        subscription.request(1);
    }

    /**
     * 只消费增、删、改、删表事件，其它事件暂不支持且会被忽略
     *
     * @param entries CanalEntry.Entry
     */
    private List<CanalEntry.Entry> filter(List<CanalEntry.Entry> entries) {
        if (CollectionUtils.isEmpty(entries)) {
            return Collections.emptyList();
        }
        return entries.stream()
                .filter(entry -> entry.getEntryType() != CanalEntry.EntryType.TRANSACTIONBEGIN)
                .filter(entry -> entry.getEntryType() != CanalEntry.EntryType.TRANSACTIONEND)
                .filter(entry -> EVENT_TYPES.contains(entry.getHeader().getEventType()))
                .collect(Collectors.toList());
    }

    @Override
    public void onError(Throwable throwable) {
        log.error(throwable.getLocalizedMessage(), throwable);
    }

    @Override
    public void onComplete() {
        log.info("Done");
    }
}
