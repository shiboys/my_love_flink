package com.swj.sensors.flink_study.streamapi.pu_uv_computing;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/01 22:28
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class UserBehaviorEvent {
  private Integer userId;
  private Integer itemId;
  private String categoryId;
  private String action;

  private Long ts;


}
