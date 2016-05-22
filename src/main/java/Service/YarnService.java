package Service;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;


/**
 * Created by zjq on 16/5/19.
 */
@Service
public class YarnService  {

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    Job tweetTopHashtags;


    public void run(String... args) throws Exception {
        System.out.println("RUNNING ..." + args[0]);
        jobLauncher.run(tweetTopHashtags, new JobParametersBuilder().toJobParameters());
    }

//    private class task implements CommandLineRunner {
//
//        public void run(String... args) throws Exception {
//            System.out.println("RUNNING ..." + args[0]);
//            jobLauncher.run(tweetTopHashtags, new JobParametersBuilder().toJobParameters());
//        }
//    }


}
