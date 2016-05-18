package Controller;


import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import java.util.concurrent.atomic.AtomicLong;

@Controller
public class GreetingController {

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();

    //    @RequestMapping("/greeting")
//    public ModelAndView greeting(@RequestParam(value = "name", defaultValue = "World") String name) throws Exception {
//////        return new Greeting(counter.incrementAndGet(),
//////                String.format(template, name));
////        return new ModelAndView("index");
//    }
    @RequestMapping("/hello")
    public String hello() {
        ModelAndView mv = new ModelAndView();
        mv.addObject("spring", "spring mvc");
        mv.setViewName("hello");
        return "hello";
    }
}