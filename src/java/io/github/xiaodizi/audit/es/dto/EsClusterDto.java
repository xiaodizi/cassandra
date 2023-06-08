package io.github.xiaodizi.audit.es.dto;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class EsClusterDto {

    private String cluster_name;
    private String status;
    private boolean timed_out;
    private Integer number_of_nodes;
    private Integer number_of_data_nodes;
    private boolean discovered_master;
    private boolean discovered_cluster_manager;
    private Integer active_primary_shards;
    private Integer active_shards;
    private Integer relocating_shards;
    private Integer initializing_shards;
    private Integer unassigned_shards;
    private Integer delayed_unassigned_shards;
    private Integer number_of_pending_tasks;
    private Integer number_of_in_flight_fetch;
    private Integer task_max_waiting_in_queue_millis;
    private BigDecimal active_shards_percent_as_number;
}
