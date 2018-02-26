package gdev;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;

abstract public class GroupValueSource {

  public int getFieldRepetitionCount(String field) {
    return getFieldRepetitionCount(getType().getFieldIndex(field));
  }

  public GroupValueSource getGroup(String field, int index) {
    return getGroup(getType().getFieldIndex(field), index);
  }

  public String getString(String field, int index) {
    return getString(getType().getFieldIndex(field), index);
  }

  public int getInteger(String field, int index) {
    return getInteger(getType().getFieldIndex(field), index);
  }

  public long getLong(String field, int index) {
    return getLong(getType().getFieldIndex(field), index);
  }

  public double getDouble(String field, int index) {
    return getDouble(getType().getFieldIndex(field), index);
  }

  public float getFloat(String field, int index) {
    return getFloat(getType().getFieldIndex(field), index);
  }

  public boolean getBoolean(String field, int index) {
    return getBoolean(getType().getFieldIndex(field), index);
  }

  public Binary getBinary(String field, int index) {
    return getBinary(getType().getFieldIndex(field), index);
  }

  public Binary getInt96(String field, int index) {
    return getInt96(getType().getFieldIndex(field), index);
  }

  abstract public int getFieldRepetitionCount(int fieldIndex);

  abstract public GroupValueSource getGroup(int fieldIndex, int index);

  abstract public String getString(int fieldIndex, int index);

  abstract public int getInteger(int fieldIndex, int index);

  abstract public long getLong(int fieldIndex, int index);

  abstract public double getDouble(int fieldIndex, int index);

  abstract public float getFloat(int fieldIndex, int index);

  abstract public boolean getBoolean(int fieldIndex, int index);

  abstract public Binary getBinary(int fieldIndex, int index);

  abstract public Binary getInt96(int fieldIndex, int index);

  abstract public String getValueToString(int fieldIndex, int index);

  abstract public GroupType getType();
}
