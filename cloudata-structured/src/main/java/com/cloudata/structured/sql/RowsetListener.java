package com.cloudata.structured.sql;

import com.cloudata.structured.sql.value.ValueHolder;

public interface RowsetListener {

    void beginRows() throws Exception;

    boolean gotRow(ValueHolder[] expressionValues) throws Exception;

    void endRows() throws Exception;

}
