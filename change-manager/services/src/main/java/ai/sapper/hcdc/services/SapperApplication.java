package ai.sapper.hcdc.services;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@SpringBootApplication
@ComponentScan("ai.sapper")
@Configuration
public class SapperApplication extends SpringBootServletInitializer {

    public static void main(String[] args) {
        SpringApplication.run(SapperApplication.class, args);
    }

}
