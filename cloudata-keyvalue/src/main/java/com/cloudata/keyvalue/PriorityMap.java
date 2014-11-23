package com.cloudata.keyvalue;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class PriorityMap<K> {
  final List<Entry<K>> entries = Lists.newArrayList();
  private Iterable<K> keys;

  static class Entry<K> {
    final K key;
    float priority;

    public Entry(K key, float priority) {
      this.key = key;
      this.priority = priority;
    }

    final static Comparator<Entry> COMPARE_BY_PRIORITY_DESC = new Comparator<Entry>() {
      @Override
      public int compare(Entry o1, Entry o2) {
        float priority1 = o1.priority;
        float priority2 = o2.priority;
        return Float.compare(priority2, priority1);
      }
    };
  }

  private void changePriority(K key, float delta, boolean add) {
    int found = findEntry(key);
    boolean needSort = false;
    if (found == -1) {
      Entry<K> entry = new Entry<K>(key, delta);
      entries.add(entry);
      needSort = true;
    } else {
      Entry<K> entry = entries.get(found);
      float newPriority = add ? entry.priority : 0;
      newPriority += delta;
      if (entry.priority == newPriority) {
        return;
      }
      entry.priority = newPriority;
      if (found > 0 && entries.get(found - 1).priority < newPriority) {
        needSort = true;
      }
      if ((found + 1 < entries.size()) && entries.get(found + 1).priority > newPriority) {
        needSort = true;
      }
    }

    if (needSort) {
      Collections.sort(entries, Entry.COMPARE_BY_PRIORITY_DESC);
      keys = null;
    }
  }

  public void setPriority(K key, float newPriority) {
    changePriority(key, newPriority, false);
  }

  public void addPriority(K key, float addPriority) {
    changePriority(key, addPriority, true);
  }

  private int findEntry(K key) {
    int found = -1;
    for (int i = 0; i < entries.size(); i++) {
      if (entries.get(i).key.equals(key)) {
        found = i;
        break;
      }
    }
    return found;
  }

  public static <K> PriorityMap<K> create() {
    return new PriorityMap<K>();
  }

  public Iterable<K> keys() {
    if (keys == null) {
      keys = ImmutableList.copyOf(Iterables.transform(entries, new Function<Entry<K>, K>() {

        @Override
        public K apply(Entry<K> input) {
          return input.key;
        }

      }));
    }
    return keys;
  }

  public K getHighest() {
    if (entries.isEmpty()) {
      return null;
    }
    return entries.get(0).key;
  }

}
