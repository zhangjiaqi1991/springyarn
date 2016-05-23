package Service;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.embedded.EmbeddedServletContainerFactory;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.hadoop.batch.scripting.ScriptTasklet;
import org.springframework.data.hadoop.batch.spark.SparkYarnTasklet;
import org.springframework.data.hadoop.scripting.HdfsScriptRunner;
import org.springframework.scripting.ScriptSource;
import org.springframework.scripting.support.ResourceScriptSource;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by hy on 16-5-11.
 */
@Configuration
@EnableBatchProcessing
public class SparkYarnConfiguration {

    @Autowired
    private org.apache.hadoop.conf.Configuration hadoopConfiguration;


    @Value("${example.inputDir}")
    String inputDir;

    @Value("${example.inputFileName}")
    String inputFileName;

    @Value("${example.inputLocalDir}")
    String inputLocalDir;

    @Value("${example.outputDir}")
    String outputDir;

    @Value("${example.sparkAssembly}")
    String sparkAssembly;

    // Job definition
    @Bean
    Job tweetHashtags(JobBuilderFactory jobs, Step initScript, Step sparkTopHashtags) throws Exception {
        System.out.println("job definition");
        return jobs.get("TweetTopHashtags")
                .start(initScript)//定义两部操作 initScript 和 sparkTopHashtags
                .next(sparkTopHashtags)
                .build();
    }

 //    Step 1 - Init Script
    @Bean
    Step initScript(StepBuilderFactory steps, Tasklet scriptTasklet) throws Exception {
        System.out.println("init scripttask");
        return steps.get("initScript")
                .tasklet(scriptTasklet)
                .build();
    }

    @Bean
    ScriptTasklet scriptTasklet(HdfsScriptRunner scriptRunner) {
        ScriptTasklet scriptTasklet = new ScriptTasklet();
        scriptTasklet.setScriptCallback(scriptRunner);
        return scriptTasklet;
    }

    @Bean
    HdfsScriptRunner scriptRunner() {
        ScriptSource script = new ResourceScriptSource(new ClassPathResource("fileCopy.js"));
        HdfsScriptRunner scriptRunner = new HdfsScriptRunner();
        scriptRunner.setConfiguration(hadoopConfiguration);
        scriptRunner.setLanguage("javascript");
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("source", inputLocalDir);
        arguments.put("file", inputFileName);
        arguments.put("indir", inputDir);
        arguments.put("outdir", outputDir);
        scriptRunner.setArguments(arguments);
        scriptRunner.setScriptSource(script);
        return scriptRunner;
    }
    // Step 2 - Spark Top Hashtags
    @Bean
    Step sparkTopHashtags(StepBuilderFactory steps, Tasklet sparkTopHashtagsTasklet) throws Exception {
        System.out.println("init sparktask");
        return steps.get("sparkTopHashtags")
                .tasklet(sparkTopHashtagsTasklet)
                .build();
    }

    @Bean
    SparkYarnTasklet sparkTopHashtagsTasklet() throws Exception {
        SparkYarnTasklet sparkTasklet = new SparkYarnTasklet();
        sparkTasklet.setSparkAssemblyJar(sparkAssembly);
        sparkTasklet.setHadoopConfiguration(hadoopConfiguration);
        sparkTasklet.setAppClass("Hashtags");
       // File jarFile = new File(System.getProperty("user.dir") + "/app/cbdp.jar");
        File jarFile = new File("/home/hy/app/cbdp.jar");
        sparkTasklet.setAppJar(jarFile.toURI().toString());
        sparkTasklet.setExecutorMemory("256M");
        sparkTasklet.setNumExecutors(1);
        sparkTasklet.setArguments(new String[]{
                hadoopConfiguration.get("fs.defaultFS") + inputDir + "/" + inputFileName,
                hadoopConfiguration.get("fs.defaultFS") + outputDir});
        return sparkTasklet;
    }

//    @Bean
//    public EmbeddedServletContainerFactory servletContainer() {
//        TomcatEmbeddedServletContainerFactory factory = new TomcatEmbeddedServletContainerFactory();
//        factory.setPort(7777);
//        factory.setSessionTimeout(10, TimeUnit.MINUTES);
//        //factory.addErrorPages(new ErrorPage(HttpStatus.404, "/notfound.html"));
//        return factory;
//    }
}
