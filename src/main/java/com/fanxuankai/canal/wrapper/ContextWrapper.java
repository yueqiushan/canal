package com.fanxuankai.canal.wrapper;

import com.fanxuankai.canal.flow.Context;
import lombok.Getter;
import lombok.Setter;

/**
 * @author fanxuankai
 */
public class ContextWrapper {
    private Context raw;
    @Getter
    private MessageWrapper messageWrapper;
    @Getter
    @Setter
    private boolean processed;
    @Getter
    private int allRawRowDataCount;

    public ContextWrapper(Context raw) {
        this.raw = raw;
        this.messageWrapper = new MessageWrapper(raw.getMessage());
        this.allRawRowDataCount = this.messageWrapper.getEntryWrapperList()
                .stream()
                .map(EntryWrapper::getRawRowDataCount)
                .reduce(Integer::sum)
                .orElse(0);

    }

    public void confirm() {
        if (processed) {
            raw.ack();
        } else {
            raw.rollback();
        }
    }
}
