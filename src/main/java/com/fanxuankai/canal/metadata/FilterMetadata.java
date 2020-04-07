package com.fanxuankai.canal.metadata;

import com.fanxuankai.canal.annotation.Filter;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;

/**
 * Filter 注解元数据
 *
 * @author fanxuankai
 */
@Getter
public class FilterMetadata {
    private String aviatorExpression;
    private List<String> updatedFields;

    public FilterMetadata(Filter filter) {
        this.aviatorExpression = filter.aviatorExpression();
        this.updatedFields = Arrays.asList(filter.updatedFields());
    }
}
