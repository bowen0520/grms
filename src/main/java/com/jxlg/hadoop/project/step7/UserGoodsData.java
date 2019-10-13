package com.jxlg.hadoop.project.step7;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 * @program: grms
 * @package: com.jxlg.hadoop.project.step7
 * @filename: UserGoodsData.java
 * @create: 2019/10/13 10:54
 * @author: 29314
 * @description: .用户商品推荐数据类型
 **/

public class UserGoodsData implements DBWritable, WritableComparable<UserGoodsData> {
    private Text userid;
    private Text goodsid;
    private IntWritable level;

    public UserGoodsData() {
        this.userid = new Text();
        this.goodsid = new Text();
        this.level = new IntWritable();
    }

    @Override
    public int compareTo(UserGoodsData o) {
        int cu = this.userid.compareTo(o.userid);
        int cg = this.goodsid.compareTo(o.goodsid);
        return cu==0?cg:cu;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserGoodsData that = (UserGoodsData) o;
        return Objects.equals(userid, that.userid) &&
                Objects.equals(goodsid, that.goodsid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userid, goodsid);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.userid.write(dataOutput);
        this.goodsid.write(dataOutput);
        this.level.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.userid.readFields(dataInput);
        this.goodsid.readFields(dataInput);
        this.level.readFields(dataInput);
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1,userid.toString());
        preparedStatement.setString(2,goodsid.toString());
        preparedStatement.setInt(3,level.get());
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.userid.set(resultSet.getString(1));
        this.goodsid.set(resultSet.getString(2));
        this.level.set(resultSet.getInt(3));
    }

    public String getUserid() {
        return userid.toString();
    }

    public void setUserid(String userid) {
        this.userid.set(userid);
    }

    public String getGoodsid() {
        return goodsid.toString();
    }

    public void setGoodsid(String goodsid) {
        this.goodsid.set(goodsid);
    }

    public int getLevel() {
        return level.get();
    }

    public void setLevel(int level) {
        this.level.set(level);
    }
}
