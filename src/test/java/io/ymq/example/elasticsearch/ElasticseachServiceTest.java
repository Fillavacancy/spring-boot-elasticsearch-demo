/**
 * FileName: ElasticseachServiceTest
 * Author:   xiangjunzhong
 * Date:     2017/11/24 14:38
 * Description:
 */
package io.ymq.example.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import io.ymq.example.elasticsearch.async.AsyncTaskConfig;
import io.ymq.example.elasticsearch.async.AsyncTaskService;
import io.ymq.example.elasticsearch.model.Corpus;
import io.ymq.example.elasticsearch.service.ElasticsearchService;
import io.ymq.example.elasticsearch.utils.ElasticsearchUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 〈一句话功能简述〉<br>
 * 〈〉
 *
 * @author xiangjunzhong
 * @create 2017/11/24 14:38
 * @since 1.0.0
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = ElasticsearchStartup.class)
public class ElasticseachServiceTest {

    @Autowired
    private ElasticsearchService elasticsearchService;

    @Autowired
    private TransportClient client;

    @Test
    public void createIndexTest() {
        elasticsearchService.createIndex("service");
    }


    @Test
    // 测试有返回结果
    public void testReturn() throws InterruptedException, ExecutionException {
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

    @Test
    public void addData() {
        for (int i = 0; i < 20; i++) {
            Corpus corpus = new Corpus();
            corpus.setId("id-" + i);
            corpus.setOriginal("原文" + i);
            corpus.setTranslation("译文" + i);
            elasticsearchService.addData(corpus, "service", "full");
        }
    }

    @Test
    public void addDatas() {
        List<Corpus> corpuses = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            Corpus corpus = new Corpus();
            corpus.setId("id--" + i);
            corpus.setOriginal("原文" + i);
            corpus.setTranslation("译文" + i);
            corpuses.add(corpus);
        }
        elasticsearchService.addData(corpuses, "service", "full");
    }

    @Test
    public void sum() {
        SumAggregationBuilder sum = AggregationBuilders
                .sum("agg")
                .field("boostFactor");
        SearchResponse sr = client.prepareSearch().addAggregation(sum).execute().actionGet();
        Sum agg = sr.getAggregations().get("agg");
        double value = agg.getValue();
        System.out.println(value);
    }

    @Test
    public void deleteDataById() {
        elasticsearchService.deleteDataById("service", "full", "id-1");
    }

    @Test
    public void updateDataById() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("original", "原文121221");
        map.put("translation", "译文21121221");

        elasticsearchService.updateDataById(ElasticsearchUtils.toJson(map), "service", "full", "id--9");
    }

    @Test
    public void searchDataById() {
        Corpus corpus = elasticsearchService.searchDataById("service", "full", "id--9", "");
        System.out.println(corpus);
    }

    @Test
    public void searchListData() {

        List<Map<String, Object>> list = elasticsearchService.searchListData("service", "full", "", "", Boolean.TRUE, null);

        for (Map<String, Object> item : list) {
            System.out.println(JSONObject.toJSONString(item));
        }
    }

    @Test
    public void promptBoostFactor() {
        elasticsearchService.promptBoostFactor("service", "full", "id--9", 20.1f);
    }
}