package com.cloudata.structured.operation;

import com.cloudata.structured.StructuredProtocol.StructuredAction;
import com.cloudata.structured.StructuredStore;

public class StructuredOperations {

    public static StructuredOperation build(StructuredStore store, StructuredAction action) {
        StructuredOperation operation;

        switch (action.getAction()) {
        case LIST_WITH_PREFIX:
            operation = new ScanOperation(action);
            break;

        case STRUCTURED_GET:
            operation = new GetOperation(action);
            break;

        case STRUCTURED_DELETE:
            operation = new StructuredDeleteOperation(action);
            break;

        case STRUCTURED_COMPOUND: {
            operation = new CompoundOperation(action, store);
            break;
        }

        case STRUCTURED_SET:
            operation = new StructuredSetOperation(action, store);
            break;

        default:
            throw new UnsupportedOperationException();
        }

        return operation;
    }

    public static boolean isReadOnly(StructuredAction action) {
        switch (action.getAction()) {
        case LIST_WITH_PREFIX:
        case STRUCTURED_GET:
            return true;

        case STRUCTURED_COMPOUND:
            for (StructuredAction child : action.getChildrenList()) {
                if (!isReadOnly(child)) {
                    return false;
                }
            }
            return true;

        default:
            return false;
        }
    }
}
