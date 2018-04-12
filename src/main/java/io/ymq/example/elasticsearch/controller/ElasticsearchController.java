/**
 * FileName: ElasticsearchController
 * Author:   xiangjunzhong
 * Date:     2017/11/29 9:30
 * Description: Elasticsearch 控制器层
 */
package io.ymq.example.elasticsearch.controller;

import io.ymq.example.elasticsearch.model.Corpus;
import io.ymq.example.elasticsearch.model.JsonVo;
import io.ymq.example.elasticsearch.service.ElasticsearchService;
import io.ymq.example.elasticsearch.utils.ElasticsearchUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 〈一句话功能简述〉<br>
 * 〈Elasticsearch 控制器层〉
 *
 * @author xiangjunzhong
 * @create 2017/11/29 9:30
 * @since 1.0.0
 */
@RestController
public class ElasticsearchController {

    /**
     * logger
     */
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchController.class);


    @Autowired
    private ElasticsearchService elasticsearchService;

    /**
     * 创建索引
     *
     * @param index
     */
    @RequestMapping(value = "/createIndex")
    public void createIndex(String index) {
        elasticsearchService.createIndex(index);
    }

    /**
     * 删除索引
     *
     * @param index
     */
    @RequestMapping(value = "/deleteIndex")
    public void deleteIndex(String index) {
        elasticsearchService.deleteIndex(index);
    }

    /**
     * 数据添加，指定ID
     *
     * @param corpus 要增加的数据
     * @param index  索引，类似数据库
     * @param type   类型，类似表
     */
    @RequestMapping(value = "/addData")
    public void addData(Corpus corpus, String index, String type) {
        elasticsearchService.addData(corpus, index, type);
    }

    /**
     * 数据添加 批量添加
     *
     * @param corpuses 要增加的数据
     * @param index    索引，类似数据库
     * @param type     型，类似表
     */
    @RequestMapping(value = "/addDatas")
    public void addData(List<Corpus> corpuses, String index, String type) {
        elasticsearchService.addData(corpuses, index, type);
    }

    /**
     * 通过ID删除数据
     *
     * @param index 索引，类似数据库
     * @param type  类型，类似表
     * @param id    数据ID
     */
    @RequestMapping(value = "/deleteDataById")
    public void deleteDataById(String index, String type, String id) {
        elasticsearchService.deleteDataById(index, type, id);
    }

    /**
     * 通过ID 更新数据
     *
     * @param corpus 要增加的数据
     * @param index  索引，类似数据库
     * @param type   类型，类似表
     * @param id     ID
     */
    @RequestMapping(value = "/updateDataById")
    public void updateDataById(Corpus corpus, String index, String type, String id) {
        elasticsearchService.updateDataById(ElasticsearchUtils.toJson(corpus), index, type, id);
    }

    /**
     * 提升权重
     *
     * @param index       索引，类似数据库
     * @param type        类型，类似表
     * @param id          数据ID
     * @param boostFactor 提升权重
     */
    @RequestMapping(value = "/promptBoostFactor")
    public void promptBoostFactor(String index, String type, String id, float boostFactor) {
        elasticsearchService.promptBoostFactor(index, type, id, boostFactor);
    }

    /**
     * 通过ID获取数据
     *
     * @param index  索引，类似数据库
     * @param type   类型，类似表
     * @param id     数据ID
     * @param fields 需要显示的字段，逗号分隔（缺省为全部字段）
     * @return
     */
    @RequestMapping(value = "/searchDataById")
    public Corpus searchDataById(String index, String type, String id, String fields) {
        return elasticsearchService.searchDataById(index, type, id, fields);
    }
}