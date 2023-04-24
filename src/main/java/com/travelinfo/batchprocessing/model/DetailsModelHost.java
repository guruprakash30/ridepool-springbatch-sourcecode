package com.travelinfo.batchprocessing.model;


import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class DetailsModelHost implements Serializable {
    private Integer details_host_id;
    private Integer waitTimeInDays;
}
