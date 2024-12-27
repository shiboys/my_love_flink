package com.swj.sensors.flink_study.statebackend.broadcaststate.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/10 12:29
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Data
public class Rule {
  private String name;
  private Shape first;
  private Shape second;
}
