package com.swj.sensors.flink_study.statebackend.broadcaststate.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/10 12:27
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
/**
 * Item 可以认为是商品，但是 Flink 官方示例把它解释为 图形
 */
public class Item {
  private Color color;
  private Shape shape;
}
