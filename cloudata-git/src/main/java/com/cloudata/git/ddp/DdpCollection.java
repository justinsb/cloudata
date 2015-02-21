package com.cloudata.git.ddp;

import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.justinsb.ddpserver.Jsonable;

public class DdpCollection {

  private static final Logger log = LoggerFactory.getLogger(DdpCollection.class);

  final String name;

  final Map<String, Jsonable> items = Maps.newHashMap();

  public DdpCollection(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public Set<Entry<String, Jsonable>> getItems() {
    return items.entrySet();
  }

  public JsonElement insert(JsonObject item) {
    String id = null;
    {
      JsonElement idElement = item.get("_id");
      if (idElement != null) {
        id = idElement.getAsString();
      }
    }
    if (id == null) {
      id = UUID.randomUUID().toString();
    }
    if (items.containsKey(id)) {
      throw new IllegalArgumentException();
    }
    JsonObject store = clone(item);
    if (store.has("_id")) {
      store.remove("_id");
    }
    items.put(id, Jsonable.fromJson(store));

    if (!item.has("_id")) {
      item.addProperty("_id", id);
    }

    // return item;
    return new JsonPrimitive(id);
  }

  public JsonElement update(JsonArray params) {
    log.info("Got update: {}", params);

    if (params.size() != 3) {
      throw new IllegalArgumentException();
    }

    JsonObject selector = params.get(0).getAsJsonObject();
    JsonObject mutator = params.get(1).getAsJsonObject();
    JsonObject options = params.get(2).getAsJsonObject();

    if (!options.entrySet().isEmpty()) {
      log.warn("Unhandled option: {}", options);
      throw new UnsupportedOperationException();
    }

    String idSelector = selectorToId(selector);
    if (idSelector == null) {
      throw new IllegalStateException("Unsupported selector (no _id): " + selector);
    }

    Jsonable item = items.get(idSelector);

    JsonObject newItem = clone((JsonObject) item.toJsonElement());

    // TODO: Don't special case everything!
    if (item != null) {
      for (Entry<String, JsonElement> entry : mutator.entrySet()) {
        String key = entry.getKey();
        if (key.equals("$inc")) {
          JsonObject spec = entry.getValue().getAsJsonObject();
          for (Entry<String, JsonElement> specEntry : spec.entrySet()) {
            String fieldName = specEntry.getKey();
            int delta = specEntry.getValue().getAsInt();

            JsonElement fieldValue = newItem.get(fieldName);
            int existing = 0;
            if (fieldValue != null) {
              existing = fieldValue.getAsInt();
            }
            newItem.addProperty(fieldName, existing + delta);
          }
        } else if (key.equals("$set")) {
          JsonObject spec = entry.getValue().getAsJsonObject();
          for (Entry<String, JsonElement> specEntry : spec.entrySet()) {
            String fieldName = specEntry.getKey();
            JsonElement newValue = specEntry.getValue();

            newItem.add(fieldName, newValue);
          }
        } else {
          throw new IllegalArgumentException("Unsupported mutator: " + mutator);
        }
      }

      items.put(idSelector, Jsonable.fromJson(newItem));
    }

    // XXX: What is the result?
    JsonObject result = new JsonObject();
    return result;
  }

  String selectorToId(JsonObject selector) {
    String idSelector = null;
    for (Entry<String, JsonElement> entry : selector.entrySet()) {
      String key = entry.getKey();
      if (key.equals("_id")) {
        idSelector = entry.getValue().getAsString();
      } else {
        throw new IllegalArgumentException("Unsupported selector: " + selector);
      }
    }

    return idSelector;
  }

  public JsonElement remove(JsonArray params) {
    log.info("Got remove: {}", params);

    if (params.size() != 1) {
      throw new IllegalArgumentException();
    }

    JsonObject selector = params.get(0).getAsJsonObject();

    String idSelector = selectorToId(selector);
    if (idSelector == null) {
      throw new IllegalStateException("Unsupported selector (no _id): " + selector);
    }

    Jsonable removed = items.remove(idSelector);
    if (removed != null) {
      log.debug("Removed item {}", idSelector);
    }
    // XXX: What is the result?
    JsonObject result = new JsonObject();
    return result;
  }

  private JsonObject clone(JsonObject item) {
    // TODO: Use deepCopy
    return (JsonObject) new JsonParser().parse(item.toString());
  }
}