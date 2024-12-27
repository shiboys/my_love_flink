package com.swj.sensors.flink_study.statebackend.broadcaststate.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/07 20:12
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Action {

  private Long userId;
  // 动作。
  private String action;

}
