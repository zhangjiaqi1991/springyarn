package Controller;

import Service.SparkYarnConfiguration;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;

import javax.servlet.http.HttpServletRequest;
import java.util.Enumeration;
import java.util.List;

/**
 * Created by zjq on 16/5/21.
 */
@Controller
public class JobLauncherController {
    private static final String JOB_PARAM="job";
    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobRegistry jobRegistry;
    @Autowired
    Job tweetTopHashtags;

//    public JobLauncherController(JobLauncher jobLauncher,JobRegistry jobRegistry){
//        super();
//        this.jobLauncher=jobLauncher;
//        this.jobRegistry=jobRegistry;
//    }
    @RequestMapping(value = "joblauncher",method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void launch(@RequestParam String job, HttpServletRequest request) throws Exception{
        JobParametersBuilder builder=extractParameters(request);//解析参数
        System.out.println(builder.toJobParameters());
        jobLauncher.run(tweetTopHashtags,new JobParametersBuilder().toJobParameters());
    }

    private JobParametersBuilder extractParameters(HttpServletRequest request){
        JobParametersBuilder builder=new JobParametersBuilder();
        Enumeration<String> paramNames=request.getParameterNames();
        while (paramNames.hasMoreElements()){
            String paramName=paramNames.nextElement();
            if(!JOB_PARAM.equals(paramName)) {
                builder.addString(paramName, request.getParameter(paramName));
            }
        }
        return builder;
    }
}
