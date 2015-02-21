package com.cloudata.datastore.dynamodb;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.base.Objects;

public class DynamoDbHelpers {

  public static boolean areEqual(Map<String, AttributeValue> l, Map<String, AttributeValue> r) {
    if (l.size() != r.size()) {
      return false;
    }

    for (String key : l.keySet()) {
      if (!r.containsKey(key)) {
        return false;
      }
    }

    // OK - keys are the same!
    for (String key : l.keySet()) {
      AttributeValue lValue = l.get(key);
      AttributeValue rValue = r.get(key);
      if (!Objects.equal(lValue, rValue)) {
        return false;
      }
    }
    return true;
  }

}
