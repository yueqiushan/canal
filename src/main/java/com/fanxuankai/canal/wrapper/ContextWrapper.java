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

    public ContextWrapper(Context raw) {
        this.raw = raw;
        this.messageWrapper = new MessageWrapper(raw.getMessage());
    }

    public void confirm() {
        if (processed) {
            raw.ack();
        } else {
            raw.rollback();
        }
    }
}
