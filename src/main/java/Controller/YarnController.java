package Controller;

import Service.YarnService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

/**
 * Created by hy on 16-5-17.
 */
@Controller
public class YarnController {
    @Autowired
    YarnService yarnService;

    @RequestMapping("/hello")
    public String hello() {
        ModelAndView mv = new ModelAndView();
        try{
            yarnService.run();
        }catch (Exception e){
            System.out.println(e.getCause());
        }
        return "hello";
    }

    @RequestMapping("/")
    public String index() {
        ModelAndView mv = new ModelAndView();
        return "index";
    }
}
