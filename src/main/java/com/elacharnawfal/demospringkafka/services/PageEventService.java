package com.elacharnawfal.demospringkafka.services;

import com.elacharnawfal.demospringkafka.entities.PageEvent;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.autoconfigure.pulsar.PulsarProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

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
}
