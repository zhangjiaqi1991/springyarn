package Service;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Service;


/**
 * Created by zjq on 16/5/19.
 */
@Service
public class YarnService {

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    Job sparkClusterJob;


    public void run(Job job, JobParametersBuilder builder) throws Exception {
        System.out.println("RUNNING ...");
        jobLauncher.run(job, builder.toJobParameters());
    }

    public void run(String... args) throws Exception {
        System.out.println("running..." + args[0]);
        jobLauncher.run(sparkClusterJob,
                new JobParametersBuilder()
                        .addString("latcol", "2")
                        .addString("loncol", "3")
                        .addString("numexecuter", "1")
                        .addString("memory", "256m")
                        .addString("filename","test.csv")
                        .addString("threshold","20")
                        .toJobParameters());
    }


}
