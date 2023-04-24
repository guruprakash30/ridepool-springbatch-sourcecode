package com.travelinfo.batchprocessing.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Setter
@Getter
public class DetailsModelPool implements Serializable {

    private Integer details_pool_id;
    private Integer waitTimeInDays;
}
