/**
 * FileName: ElasticsearchService
 * Author:   xiangjunzhong
 * Date:     2017/11/22 9:36
 * Description: Elasticsearch 操作
 */
package io.ymq.example.elasticsearch.service;

import com.alibaba.fastjson.JSONObject;
import io.ymq.example.elasticsearch.model.Corpus;
import io.ymq.example.elasticsearch.model.EsPage;

import java.util.List;
import java.util.Map;

/**
 * 〈一句话功能简述〉<br>
 * 〈Elasticsearch 操作〉
 *
 * @author xiangjunzhong
 * @create 2017/11/22 9:36
 * @since 1.0.0
 */
public interface ElasticsearchService {

    /**
     * 创建索引
     *
     * @param index
     * @return
     */
    public boolean createIndex(String index);

    /**
     * 删除索引
     *
     * @param index
     * @return
     */
    public boolean deleteIndex(String index);


    /**
     * 判断索引是否存在
     *
     * @param index
     * @return
     */
    public boolean isIndexExist(String index);

    /**
     * 数据添加，指定ID
     *
     * @param corpus 要增加的数据
     * @param index  索引，类似数据库
     * @param type   类型，类似表
     * @return
     */
    public String addData(Corpus corpus, String index, String type);

    /**
     * 数据添加 批量添加
     *
     * @param corpuses 要增加的数据
     * @param index    索引，类似数据库
     * @param type     型，类似表
     * @return
     */
    public void addData(List<Corpus> corpuses, String index, String type);

    /**
     * 通过ID删除数据
     *
     * @param index 索引，类似数据库
     * @param type  类型，类似表
     * @param id    数据ID
     */
    public void deleteDataById(String index, String type, String id);

    /**
     * 通过ID 更新数据
     *
     * @param jsonObject 要增加的数据
     * @param index      索引，类似数据库
     * @param type       类型，类似表
     * @param id         ID
     * @return
     */
    public void updateDataById(JSONObject jsonObject, String index, String type, String id);

    /**
     * 提升权重
     *
     * @param index       索引，类似数据库
     * @param type        类型，类似表
     * @param id          数据ID
     * @param boostFactor 提升权重
     * @return
     */
    public void promptBoostFactor(String index, String type, String id, float boostFactor);

    /**
     * 通过ID获取数据
     *
     * @param index  索引，类似数据库
     * @param type   类型，类似表
     * @param id     数据ID
     * @param fields 需要显示的字段，逗号分隔（缺省为全部字段）
     * @return
     */
    public Corpus searchDataById(String index, String type, String id, String fields);

    /**
     * 使用分词查询
     *
     * @param index    索引名称
     * @param type     类型名称,可传入多个type逗号分隔
     * @param fields   需要显示的字段，逗号分隔（缺省为全部字段）
     * @param matchStr 过滤条件（xxx=111,aaa=222）
     * @return
     */
    public List<Map<String, Object>> searchListData(String index, String type, String fields, String matchStr);

    /**
     * 使用分词查询
     *
     * @param index       索引名称
     * @param type        类型名称,可传入多个type逗号分隔
     * @param fields      需要显示的字段，逗号分隔（缺省为全部字段）
     * @param sortField   排序字段
     * @param matchPhrase true 使用，短语精准匹配
     * @param matchStr    过滤条件（xxx=111,aaa=222）
     * @return
     */
    public List<Map<String, Object>> searchListData(String index, String type, String fields, String sortField, boolean matchPhrase, String matchStr);

    /**
     * 使用分词查询
     *
     * @param index          索引名称
     * @param type           类型名称,可传入多个type逗号分隔
     * @param size           文档大小限制
     * @param fields         需要显示的字段，逗号分隔（缺省为全部字段）
     * @param sortField      排序字段
     * @param matchPhrase    true 使用，短语精准匹配
     * @param highlightField 高亮字段
     * @param matchStr       过滤条件（xxx=111,aaa=222）
     * @return
     */
    public List<Map<String, Object>> searchListData(String index, String type, Integer size, String fields, String sortField, boolean matchPhrase, String highlightField, String matchStr);


    /**
     * 使用分词查询
     *
     * @param index          索引名称
     * @param type           类型名称,可传入多个type逗号分隔
     * @param startTime      开始时间
     * @param endTime        结束时间
     * @param size           文档大小限制
     * @param fields         需要显示的字段，逗号分隔（缺省为全部字段）
     * @param sortField      排序字段
     * @param matchPhrase    true 使用，短语精准匹配
     * @param highlightField 高亮字段
     * @param matchStr       过滤条件（xxx=111,aaa=222）
     * @return
     */
    public List<Map<String, Object>> searchListData(String index, String type, long startTime, long endTime, Integer size, String fields, String sortField, boolean matchPhrase, String highlightField, String matchStr);


    /**
     * 使用分词查询,并分页
     *
     * @param index          索引名称
     * @param type           类型名称,可传入多个type逗号分隔
     * @param currentPage    当前页
     * @param pageSize       每页显示条数
     * @param startTime      开始时间
     * @param endTime        结束时间
     * @param fields         需要显示的字段，逗号分隔（缺省为全部字段）
     * @param sortField      排序字段
     * @param matchPhrase    true 使用，短语精准匹配
     * @param highlightField 高亮字段
     * @param matchStr       过滤条件（xxx=111,aaa=222）
     * @return
     */
    public EsPage searchDataPage(String index, String type, int currentPage, int pageSize, long startTime, long endTime, String fields, String sortField, boolean matchPhrase, String highlightField, String matchStr);
}