package io.github.changebooks.kafka.sample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author changebooks@qq.com
 */
@SpringBootApplication
public class Application {

    public static final String TOPIC = "topic001";

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}
