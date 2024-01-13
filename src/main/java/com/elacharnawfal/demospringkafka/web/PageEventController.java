package com.elacharnawfal.demospringkafka.web;

import com.elacharnawfal.demospringkafka.entities.PageEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Random;

@RestController
@RequiredArgsConstructor
public class PageEventController {
    private final StreamBridge streamBridge;

    @GetMapping("/publish/{topic}/{name}")
    public PageEvent publish(@PathVariable String topic , @PathVariable String name){

        PageEvent pageEvent = new PageEvent(name,Math.random()>0.5?"U1":"U2",new Date(),new Random().nextLong(9000));
        streamBridge.send(topic,pageEvent);
        return pageEvent;
    }
}
