package Controller;

import Service.YarnService;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;

import javax.servlet.http.HttpServletRequest;
import java.util.Enumeration;

/**
 * Created by zjq on 16/5/21.
 */
@Controller
public class JobLauncherController {
    private static final String JOB_PARAM="job";
    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    Job sparkClusterJob;

    @Autowired
    YarnService yarnService;
    //localhost:7070/joblauncher?job=sparkClusterJob&latcol=2&loncol=3&numexecuter=1&memory=256m&filename=test.csv&threshold=20
    @RequestMapping(value = "joblauncher",method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void launch(@RequestParam String job, HttpServletRequest request) throws Exception{
        JobParametersBuilder builder=extractParameters(request);//解析参数
        yarnService.run(sparkClusterJob,builder);
        //jobLauncher.run(sparkClusterJob,new JobParametersBuilder().toJobParameters());

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
