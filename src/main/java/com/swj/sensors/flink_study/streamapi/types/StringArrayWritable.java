package com.swj.sensors.flink_study.streamapi.types;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/30 17:53
 * 将一个 String 数组进行序列化
 */
public class StringArrayWritable implements Writable, Comparator<StringArrayWritable> {

  private String[] stringArray = new String[0];

  public StringArrayWritable(String[] stringArray) {
    this.stringArray = stringArray;
  }

  public StringArrayWritable() {
    super();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(stringArray.length);
    for (String str : stringArray) {
      byte[] b = str.getBytes(ConfigConstants.DEFAULT_CHARSET);
      dataOutput.writeInt(b.length);
      dataOutput.write(b);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int len = dataInput.readInt();
    String[] tmpArr = new String[len];
    for (int i = 0; i < len; i++) {
      byte[] b = new byte[dataInput.readInt()];
      dataInput.readFully(b);
      tmpArr[i] = new String(b, ConfigConstants.DEFAULT_CHARSET);
    }
    this.stringArray = tmpArr;
  }

  @Override
  public int compare(StringArrayWritable o1, StringArrayWritable o2) {

    // 先比较数组长度
    if (o1.stringArray.length != o2.stringArray.length) {
      return o1.stringArray.length - o2.stringArray.length;
    }
    // 在比较每个字符串元素的长度。此处 字符串数组 的元素个数相同
    for (int i = 0, len = o1.stringArray.length; i < len; i++) {
      int cmp = o1.stringArray[i].compareTo(o2.stringArray[i]);
      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof StringArrayWritable)) {
      return false;
    }
    return compare(this, (StringArrayWritable) obj) == 0;
  }
}
