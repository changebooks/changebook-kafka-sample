package io.github.changebooks.kafka.sample;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author changebooks@qq.com
 */
@RequestMapping("test")
@RestController
public class TestController {

    @Resource
    private MessageSender messageSender;

    @GetMapping(value = "/test")
    public void test() throws ExecutionException, InterruptedException {
        List<String> data = new ArrayList<>();

        data.add("0");
        data.add("1");
        data.add("2");
        data.add("3");
        data.add("4");
        data.add("5");
        data.add("6");
        data.add("7");
        data.add("8");
        data.add("9");

        messageSender.send(data);
    }

}
