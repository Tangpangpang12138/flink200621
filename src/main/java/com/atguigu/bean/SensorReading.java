package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author TangZC
 * @create 2020-11-17 10:55
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading {

    private String id;
    private Long ts;
    private Double temp;

    @Override
    public String toString() {
        return id + " , " + ts + " , " + temp;
    }
}
