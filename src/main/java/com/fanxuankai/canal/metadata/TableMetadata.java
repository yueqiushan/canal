package com.fanxuankai.canal.metadata;

import com.fanxuankai.canal.annotation.EnableCanalAttributes;
import com.fanxuankai.canal.annotation.Table;
import com.fanxuankai.canal.util.ReflectionUtils;
import lombok.Data;

import java.util.Objects;

/**
 * Table 注解元数据
 *
 * @author fanxuankai
 */
@Data
public class TableMetadata {
    private String schema;
    private String name;

    public TableMetadata(Table table, Class<?> clazz) {
        String schema = table.schema();
        this.schema = schema.isEmpty() ? EnableCanalAttributes.getSchema() : schema;
        String name = table.name();
        this.name = name.isEmpty() ? ReflectionUtils.getTableName(clazz) : name;
    }

    public TableMetadata(String schema, String name) {
        this.schema = schema;
        this.name = name;
    }

    public String toFilter() {
        return schema + "." + name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableMetadata that = (TableMetadata) o;
        return Objects.equals(schema, that.schema) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, name);
    }
}
