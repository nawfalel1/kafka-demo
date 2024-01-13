package com.elacharnawfal.demospringkafka.entities;

import lombok.*;

import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class PageEvent {

    private String name;
    private String user;
    private Date date;
    private Long duration;
}
