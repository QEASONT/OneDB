package org.apache.spark.sql.execution.odb.index.entity;

import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point;

import java.util.List;

public class MVPDP {
    private String id;
    private Point<?> data;
    private List<Double> path;
    private int maxPath;
    private int dataLen;
//    private MVPDataType type;

    // Constructor
    public MVPDP(int id, Point<?> data) {
        this.id = String.valueOf(id);
        this.data = data;
        this.path = null;
        this.dataLen = 0;
//        this.type = type;
    }

    // Getters and setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Point<?> getData() {
        return data;
    }

    public void setData(Point<?> data) {
        this.data = data;
    }

    public List<Double> getPath() {
        return path;
    }

    public void setPath(List<Double> path) {
        this.path = path;
        this.maxPath = path.size();
    }

    public void setPath(int index, Double val) {
        if (index > path.size())
            this.path.add(index - 1, 0.0d);
        this.path.add(index, val);
    }

    public int getDataLen() {
        return dataLen;
    }

    public void setDataLen(int dataLen) {
        this.dataLen = dataLen;
    }

//    public MVPDataType getType() {
//        return type;
//    }

//    public void setType(MVPDataType type) {
//        this.type = type;
//    }
}
