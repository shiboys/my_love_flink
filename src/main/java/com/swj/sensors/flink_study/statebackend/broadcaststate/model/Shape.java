package com.swj.sensors.flink_study.statebackend.broadcaststate.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Objects;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/10 12:23
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Data
public class Shape {
  public static final Shape TRIANGLE = new Shape("triangle");
  public static final Shape RECTANGLE = new Shape("rectangle");
  public static final Shape SQUARE = new Shape("square");

  private String name;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Shape shape = (Shape) o;
    return Objects.equals(name, shape.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
