package com.epam.cc.java.ftp.prototype.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
//@ImportResource("classpath:spring-integration.xml")
public class Application {
    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext ctx = new SpringApplication(FtpChannelConfig.class).run(args);
    }

}
