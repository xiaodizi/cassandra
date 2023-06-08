package io.github.xiaodizi.audit.es.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Total {
    private int value;
    private String relation;
}
