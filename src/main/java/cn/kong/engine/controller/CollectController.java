package cn.kong.engine.controller;

import cn.kong.engine.model.CrawlerTask;
import cn.kong.engine.service.CollectService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

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
    public String runCrawler(@RequestBody CrawlerTask task) {
        collectService.runCrawler(task);
        return "Crawler started in the background";
    }

    @GetMapping("/stop")
    public String stopCrawler(@RequestParam String taskId) {
        collectService.stopCrawler(taskId);
        return "Crawler stopped successfully";
    }
}