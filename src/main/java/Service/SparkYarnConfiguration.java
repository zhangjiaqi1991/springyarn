package Service;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.hadoop.batch.scripting.ScriptTasklet;
import org.springframework.data.hadoop.batch.spark.SparkYarnTasklet;
import org.springframework.data.hadoop.scripting.HdfsScriptRunner;
import org.springframework.scripting.ScriptSource;
import org.springframework.scripting.support.ResourceScriptSource;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

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

    @Value("${example.outputLocalDir}")
    String outputLocalDir;



    // Job definition
    @Bean
    Job sparkClusterJob(JobBuilderFactory jobs, Step initScript, Step sparkCluster,Step finishScript) throws Exception {
        System.out.println("job definition");
        return jobs.get("sparkClusterJob")
                .start(initScript)//定义两部操作 initScript 和 sparkTopHashtags
                .next(sparkCluster)
                .next(finishScript)
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
        ScriptSource script = new ResourceScriptSource(new ClassPathResource("fileCopyToHdfs.js"));
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
    // Step 2 - spark Cluster tasklet
    @Bean
    Step sparkCluster(StepBuilderFactory steps, Tasklet sparkClusterTasklet) throws Exception {
        System.out.println("init sparktask");
        return steps.get("sparkCluster")
                .tasklet(sparkClusterTasklet)
                .build();
    }


    @Bean
    @StepScope
    SparkYarnTasklet sparkClusterTasklet(
            @Value("#{jobParameters['latcol']}")final String latcol,//经度列
            @Value("#{jobParameters['loncol']}")final String loncol,//纬度列
            @Value("#{jobParameters['numexecuter']}")final String numexecuter,//excutor个数
            @Value("#{jobParameters['memory']}")final String memory,//内存
            @Value("#{jobParameters['filename']}")final String filename,//文件名
    @Value("#{jobParameters['threshold']}")final String threshold//阈值

    ) throws Exception {
        SparkYarnTasklet sparkTasklet = new SparkYarnTasklet();
        sparkTasklet.setSparkAssemblyJar(sparkAssembly);
        sparkTasklet.setHadoopConfiguration(hadoopConfiguration);
        sparkTasklet.setAppClass("sparkCluster");//主类
        File jarFile = new File("/Users/zjq/IdeaProjects/springyarn/app/cbdp.jar");//jar包
        sparkTasklet.setAppJar(jarFile.toURI().toString());
        sparkTasklet.setExecutorMemory(memory);
        sparkTasklet.setNumExecutors(Integer.valueOf(numexecuter));
        sparkTasklet.setArguments(new String[]{
                hadoopConfiguration.get("fs.defaultFS") + inputDir + "/" + inputFileName,
                hadoopConfiguration.get("fs.defaultFS") + outputDir,latcol,loncol,filename,threshold});
        return sparkTasklet;
    }
    // Step 3 - finish Script
    @Bean
    Step finishScript(StepBuilderFactory steps, Tasklet finishscriptTasklet) throws Exception {
        System.out.println("finish scripttask");
        return steps.get("finishScript")
                .tasklet(finishscriptTasklet)
                .build();
    }

    @Bean
    ScriptTasklet finishscriptTasklet(HdfsScriptRunner finishscriptRunner) {
        ScriptTasklet scriptTasklet = new ScriptTasklet();
        scriptTasklet.setScriptCallback(finishscriptRunner);
        return scriptTasklet;
    }

    @Bean
    HdfsScriptRunner finishscriptRunner() {
        ScriptSource script = new ResourceScriptSource(new ClassPathResource("fileCopyToFs.js"));
        HdfsScriptRunner scriptRunner = new HdfsScriptRunner();
        scriptRunner.setConfiguration(hadoopConfiguration);
        scriptRunner.setLanguage("javascript");
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("result",outputLocalDir);
        arguments.put("outdir", outputDir);//file already exists!
        scriptRunner.setArguments(arguments);
        scriptRunner.setScriptSource(script);
        return scriptRunner;
    }

}
