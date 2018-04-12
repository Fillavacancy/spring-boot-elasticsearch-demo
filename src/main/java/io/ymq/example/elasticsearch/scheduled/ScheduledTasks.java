package io.ymq.example.elasticsearch.scheduled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

@Component
public class ScheduledTasks {
    /*
        @Scheduled所支持的参数：
        1.cron：cron表达式，指定任务在特定时间执行；
        2.fixedDelay：表示上一次任务执行完成后多久再次执行，参数类型为long，单位ms；
        3.fixedDelayString：与fixedDelay含义一样，只是参数类型变为String；
        4.fixedRate：表示按一定的频率执行任务，参数类型为long，单位ms；
        5.fixedRateString: 与fixedRate的含义一样，只是将参数类型变为String；
        6.initialDelay：表示延迟多久再第一次执行任务，参数类型为long，单位ms；
        7.initialDelayString：与initialDelay的含义一样，只是将参数类型变为String；
        8.zone：时区，默认为当前时区，一般没有用到。
    */

    private Logger logger = LoggerFactory.getLogger(ScheduledTasks.class);

    @Scheduled(cron = "0/1 * * * * ? ")   //每1秒执行一次
    public void testCron1() {
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        logger.info(sdf.format(new Date()) + "*********每1秒执行一次");
    }

    @Scheduled(cron = "0/2 * * * * ? ")   //每2秒执行一次
    public void testCron2() {
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        logger.info(sdf.format(new Date()) + "*********每2秒执行一次");
    }

    @Scheduled(cron = "0/3 * * * * ? ")   //每3秒执行一次
    public void testCron3() {
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        logger.info(sdf.format(new Date()) + "*********每3秒执行一次");
    }

    @Scheduled(cron = "0/4 * * * * ? ")   //每4秒执行一次
    public void testCron4() {
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        logger.info(sdf.format(new Date()) + "*********每4秒执行一次");
    }

    // cron接受cron表达式，根据cron表达式确定定时规则
    @Scheduled(cron = "0/5 * * * * ? ")   //每5秒执行一次
    public void testCron() {
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        logger.info(sdf.format(new Date()) + "*********每5秒执行一次");
    }
}