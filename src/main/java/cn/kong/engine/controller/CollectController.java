package cn.kong.engine.controller;

import cn.kong.engine.service.CollectService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author gzkon
 * @description: TODO
 * @date 2025/6/22 21:13
 */
@RestController
@RequestMapping("/collect")  // 统一类级路径
public class CollectController {

    @Autowired
    private CollectService collectService;

    @PostMapping("/run")
    public String runCrawler(@RequestBody List<String> seeds) {
        collectService.runCrawler(seeds);
        return "Crawler started in the background";
    }
}