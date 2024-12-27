package com.swj.sensors.flink_study.statebackend.broadcaststate.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/10 12:20
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Data
public class Color {
  public static final Color RED = new Color("red");
  public static final Color GREEN = new Color("green");
  public static final Color BLUE = new Color("blue");

  private String name;
}
