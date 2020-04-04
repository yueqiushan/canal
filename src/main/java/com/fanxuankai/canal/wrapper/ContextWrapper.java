package com.fanxuankai.canal.wrapper;

import com.fanxuankai.canal.flow.Context;
import lombok.Getter;

/**
 * @author fanxuankai
 */
public class ContextWrapper {
    private Context raw;
    @Getter
    private MessageWrapper messageWrapper;

    public ContextWrapper(Context raw) {
        this.raw = raw;
        this.messageWrapper = new MessageWrapper(raw.getMessage());
    }

    public void confirm() {
        raw.ack();
    }
}
