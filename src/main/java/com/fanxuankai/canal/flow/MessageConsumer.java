package com.fanxuankai.canal.flow;

import com.fanxuankai.canal.wrapper.EntryWrapper;

/**
 * @author fanxuankai
 */
public interface MessageConsumer<R> extends Consumer<EntryWrapper, R>, Filter {
}
