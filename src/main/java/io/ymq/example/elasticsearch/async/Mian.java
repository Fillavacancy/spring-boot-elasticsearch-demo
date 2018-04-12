package io.ymq.example.elasticsearch.async;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.task.TaskRejectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Mian {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
/*        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(AsyncTaskConfig.class);

        AsyncTaskService asyncTaskService = context.getBean(AsyncTaskService.class);

        for (int i = 0; i < 10; i++) {
            asyncTaskService.executeAsyncTask(i);
            asyncTaskService.executeAsyncTaskPlus(i);
        }*/

        testReturn();
    }

    // 测试有返回结果
    public static void testReturn() throws InterruptedException, ExecutionException {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(AsyncTaskConfig.class);
        AsyncTaskService asyncTaskService = context.getBean(AsyncTaskService.class);

        List<Future<String>> lstFuture = new ArrayList<Future<String>>();// 存放所有的线程，用于获取结果

        // 创建100个线程
        for (int i = 1; i <= 10; i++) {
            while (true) {
                try {
                    // 线程池超过最大线程数时，会抛出TaskRejectedException，则等待1s，直到不抛出异常为止
                    Future<String> future = asyncTaskService.asyncInvokeReturnFuture(i);
                    lstFuture.add(future);
                    System.out.println("-------------------");
                    break;
                } catch (TaskRejectedException e) {
                    System.out.println("线程池满，等待1S。");
                    Thread.sleep(1000);
                }
            }
        }

        // 获取值。get是阻塞式，等待当前线程完成才返回值
        for (Future<String> future : lstFuture) {
            System.out.println(future.get());
        }

        context.close();
    }
}