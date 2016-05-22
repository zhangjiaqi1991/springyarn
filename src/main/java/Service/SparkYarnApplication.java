package Service;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Created by hy on 16-5-11.
 */
@SpringBootApplication
@EnableBatchProcessing
public class SparkYarnApplication implements CommandLineRunner {

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    Job tweetTopHashtags;

    public static void main(String[] args) {
        SpringApplication.run(SparkYarnApplication.class, args);
    }

    public void run(String... args) throws Exception {
        System.out.println("RUNNING ...");
        jobLauncher.run(tweetTopHashtags, new JobParametersBuilder().toJobParameters());
    }

}
