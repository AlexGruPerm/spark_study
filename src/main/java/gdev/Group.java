package gdev;

import org.apache.log4j.Logger;
import org.apache.parquet.Log;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

abstract public class Group extends GroupValueSource {
  //private static final Log logger = Log.getLog(Group.class);
	
  final static Logger logger = Logger.getLogger(Group.class);
	
  private static final boolean DEBUG = Log.DEBUG;

  public void add(String field, int value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, long value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, float value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, double value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, String value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, NanoTime value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, boolean value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, Binary value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, Group value) {
    add(getType().getFieldIndex(field), value);
  }

  public Group addGroup(String field) {
    if (DEBUG) logger.debug("add group "+field+" to "+getType().getName());
    return addGroup(getType().getFieldIndex(field));
  }

  public Group getGroup(String field, int index) {
    return getGroup(getType().getFieldIndex(field), index);
  }

  abstract public void add(int fieldIndex, int value);

  abstract public void add(int fieldIndex, long value);

  abstract public void add(int fieldIndex, String value);

  abstract public void add(int fieldIndex, boolean value);

  abstract public void add(int fieldIndex, NanoTime value);

  abstract public void add(int fieldIndex, Binary value);

  abstract public void add(int fieldIndex, float value);

  abstract public void add(int fieldIndex, double value);

  abstract public void add(int fieldIndex, Group value);

  abstract public Group addGroup(int fieldIndex);

  abstract public Group getGroup(int fieldIndex, int index);

  public Group asGroup() {
    return this;
  }

  public Group append(String fieldName, int value) {
    add(fieldName, value);
    return this;
  }

  public Group append(String fieldName, float value) {
    add(fieldName, value);
    return this;
  }

  public Group append(String fieldName, double value) {
    add(fieldName, value);
    return this;
  }

  public Group append(String fieldName, long value) {
    add(fieldName, value);
    return this;
  }

  public Group append(String fieldName, NanoTime value) {
    add(fieldName, value);
    return this;
  }

  public Group append(String fieldName, String value) {
    add(fieldName, Binary.fromString(value));
    return this;
  }

  public Group append(String fieldName, boolean value) {
    add(fieldName, value);
    return this;
  }

  public Group append(String fieldName, Binary value) {
    add(fieldName, value);
    return this;
  }

  abstract public void writeValue(int field, int index, RecordConsumer recordConsumer);

}
