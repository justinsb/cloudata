package com.cloudata.keyvalue.operation;

import com.cloudata.keyvalue.KeyValueProtocol.KeyValueAction;

public class KeyValueOperations {

    public static KeyValueOperation build(KeyValueAction action) {
        KeyValueOperation operation;

        switch (action.getAction()) {
        case LIST_ENTRIES_WITH_PREFIX:
            operation = new ScanOperation(action);
            break;

        case LIST_KEYS_WITH_PREFIX:
            operation = new ScanOperation(action);
            break;

        case APPEND:
            operation = new AppendOperation(action);
            break;

        case GET:
            operation = new GetOperation(action);
            break;

        case DELETE:
            operation = new DeleteOperation(action);
            break;

        case INCREMENT: {
            operation = new IncrementOperation(action);
            break;
        }

        case COMPOUND: {
            operation = new CompoundOperation(action);
            break;
        }

        case SET:
            operation = new SetOperation(action);
            break;

        default:
            throw new UnsupportedOperationException();
        }

        return operation;
    }

    public static boolean isReadOnly(KeyValueAction action) {
        switch (action.getAction()) {
        case GET:
            return true;

        case COMPOUND:
            for (KeyValueAction child : action.getChildrenList()) {
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
