package com.elacharnawfal.demospringkafka.services;

import com.elacharnawfal.demospringkafka.entities.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.autoconfigure.pulsar.PulsarProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Component
public class PageEventService {



//    @KafkaListener(topics = "R4",groupId = "com.elacharnawfal")
//    public void consume(PageEvent pageEvent){
//
//            System.out.println("******------************");
//            System.out.println(pageEvent.toString());
//            System.out.println("*******-----**********");
//    }

    @Bean
    public Consumer<PageEvent> pageEventConsumer(){
        return (pageEvent -> {
            System.out.println("******------************");
            System.out.println(pageEvent.toString());
            System.out.println("*******-----**********");
        });
    }
    @Bean
    public Supplier<PageEvent> pageEventSupplier(){
        return ()->
          new PageEvent(
                  Math.random()>0.5 ? "P1" : "P2",
                  Math.random()>0.5 ? "U1" : "U2",
                  new Date(),
                  new Random().nextLong(9000)
          );
        }

    @Bean
    public Function<PageEvent,PageEvent> pageEventFunction(){
        return (input)->{
            input.setName("PageEvent");
            input.setUser("Nawfal");
            return input;
        };
    }

    @Bean
    public Function<KStream<String,PageEvent>,KStream<String,Long>> kStreamFunction(){

        return (input)->{
            return input.filter((k,v)-> v.getDuration()>100)
                    .map((k,v)-> new KeyValue<>(v.getName(),0L))
                    .groupBy((k,v)->k, Grouped.with(Serdes.String(),Serdes.Long()))
                    .windowedBy(TimeWindows.of(Duration.ofSeconds(5000)))
                    .count(Materialized.as("Page-count"))
                    .toStream()
                    .map((k,v)->new KeyValue<>("=>"+k.window().startTime()+k.window().endTime()+k.key(),v));
        };
    }
}
