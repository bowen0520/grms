package com.jxlg.hadoop.project.step5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @program: grms
 * @package: com.jxlg.hadoop.project.dao
 * @filename: KeyValueData.java
 * @create: 2019/10/12 15:09
 * @author: 29314
 * @description: .自定义的数据类型
 **/

public class KeyValueData implements WritableComparable<KeyValueData> {
    private Text k;
    private IntWritable v;

    public KeyValueData() {
        this.k = new Text();
        this.v = new IntWritable();
    }

    @Override
    public int compareTo(KeyValueData o) {
        return this.k.compareTo(o.k);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyValueData that = (KeyValueData) o;
        return k.equals(that.k);
    }

    @Override
    public int hashCode() {
        return k.hashCode();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.k.write(dataOutput);
        this.v.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.k.readFields(dataInput);
        this.v.readFields(dataInput);
    }

    public String getK() {
        return k.toString();
    }

    public void setK(String k) {
        this.k.set(k);
    }

    public int getV() {
        return v.get();
    }

    public void setV(int v) {
        this.v.set(v);
    }

    @Override
    public String toString() {
        return this.k.toString();
    }
}
