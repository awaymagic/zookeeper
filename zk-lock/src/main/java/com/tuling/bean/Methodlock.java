package com.tuling.bean;

import lombok.Data;

import java.util.Date;

@Data
public class Methodlock {
    private Integer id;
    //保证唯一性
    private String methodName;

    private Date updateTime;

    public Methodlock() {
    }

    public Methodlock(String methodName) {
        this.methodName = methodName;
    }

}