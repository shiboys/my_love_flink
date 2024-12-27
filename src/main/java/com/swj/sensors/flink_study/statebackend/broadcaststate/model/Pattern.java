package com.swj.sensors.flink_study.statebackend.broadcaststate.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/07 20:15
 */

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Pattern {

  // 前一个动作和后一个动作
  private String firstAction;
  private String secondAction;
}
