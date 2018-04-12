/**
 * FileName: ElasticsearchServiceImpl
 * Author:   xiangjunzhong
 * Date:     2017/11/22 9:35
 * Description: Elasticsearch 操作实现类
 */
package io.ymq.example.elasticsearch.service.impl;

import com.alibaba.fastjson.JSONObject;
import io.ymq.example.elasticsearch.config.ElasticSearchReservedWords;
import io.ymq.example.elasticsearch.config.IndexSchemaBuilder;
import io.ymq.example.elasticsearch.config.SearchDocumentFieldName;
import io.ymq.example.elasticsearch.model.Corpus;
import io.ymq.example.elasticsearch.model.EsPage;
import io.ymq.example.elasticsearch.service.ElasticsearchService;
import io.ymq.example.elasticsearch.service.SetupIndexService;
import io.ymq.example.elasticsearch.utils.ElasticsearchUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.util.TextUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * 〈一句话功能简述〉<br>
 * Elasticsearch 操作实现类
 *
 * @author xiangjunzhong
 * @create 2017/11/22 9:35
 * @since 1.0.0
 */
@Service
public class ElasticsearchServiceImpl implements ElasticsearchService {

    /**
     * logger
     */
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchServiceImpl.class);


    @Autowired
    private TransportClient client;

    @Autowired
    private SetupIndexService setupIndexService;

    @Autowired
    private IndexSchemaBuilder indexSchemaBuilder;

    /**
     * 创建索引
     *
     * @param index
     * @return
     */
    @Override
    public boolean createIndex(String index) {
        index = ElasticsearchUtils.toLowerCase(index);

        if (isIndexExist(index)) {
            logger.info("Index is exits!");
        }

        CreateIndexResponse indexresponse = null;

        try {
            indexresponse = client.admin().indices().prepareCreate(index).setSettings(indexSchemaBuilder.getSettingForIndex(index)).execute().actionGet();
        } catch (IOException e) {
            e.printStackTrace();
        }

        setupIndexService.updateDocumentTypeMapping(index, ElasticSearchReservedWords.TYPE_NAME.getText());

        logger.info("执行建立成功？" + indexresponse.isAcknowledged());

        return indexresponse.isAcknowledged();
    }

    /**
     * 删除索引
     *
     * @param index
     * @return
     */
    @Override
    public boolean deleteIndex(String index) {
        index = ElasticsearchUtils.toLowerCase(index);

        if (!isIndexExist(index)) {
            logger.info("Index is not exits!");
            return true;
        }

        DeleteIndexResponse dResponse = client.admin().indices().prepareDelete(index).execute().actionGet();
        if (dResponse.isAcknowledged()) {
            logger.info("delete index " + index + "  successfully!");
        } else {
            logger.info("Fail to delete index " + index);
        }
        return dResponse.isAcknowledged();
    }

    /**
     * 判断索引是否存在
     *
     * @param index
     * @return
     */
    @Override
    public boolean isIndexExist(String index) {
        index = ElasticsearchUtils.toLowerCase(index);

        IndicesExistsResponse inExistsResponse = client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet();

        if (inExistsResponse.isExists()) {
            logger.info("Index [" + index + "] is exist!");
        } else {
            logger.info("Index [" + index + "] is not exist!");
        }

        return inExistsResponse.isExists();
    }

    /**
     * 数据添加
     * 指定ID or 不指定ID
     *
     * @param corpus 要增加的数据
     * @param index  索引，类似数据库
     * @param type   类型，类似表
     * @return
     */
    @Override
    public String addData(Corpus corpus, String index, String type) {
        index = ElasticsearchUtils.toLowerCase(index);

        IndexResponse response = null;

        try {
            response = getIndexRequestBuilderForACorpus(corpus, index, type).get();
        } catch (Exception ex) {
            logger.error("Error occurred while creating index document for product.", ex);
            throw new RuntimeException(ex);
        }

        logger.info("addData response status:{},id:{}", response.status().getStatus(), response.getId());

        return response.getId();
    }

    /**
     * 数据添加 批量添加
     * 集合中所有对象需要设置ID
     *
     * @param corpuses 要添加的数据
     * @param index    索引，类似数据库
     * @param type     类型，类似表
     * @return
     */
    @Override
    public void addData(List<Corpus> corpuses, String index, String type) {
        index = ElasticsearchUtils.toLowerCase(index);

        if (corpuses.isEmpty()) {
            return;
        }

        List<IndexRequestBuilder> requests = new ArrayList<IndexRequestBuilder>();

        for (Corpus corpus : corpuses) {
            try {
                requests.add(getIndexRequestBuilderForACorpus(corpus, index, type));
            } catch (Exception ex) {
                logger.error("Error occurred while creating index document for product with id: " + "" + ", moving to next product!", ex);
            }
        }
        corpusBulkRequests(requests);
    }

    /**
     * 通过ID删除数据
     *
     * @param index 索引，类似数据库
     * @param type  类型，类似表
     * @param id    数据ID
     */
    @Override
    public void deleteDataById(String index, String type, String id) {
        index = ElasticsearchUtils.toLowerCase(index);

        DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();

        logger.info("deleteDataById response status:{},id:{}", response.status().getStatus(), response.getId());
    }

    /**
     * 通过ID 更新数据
     *
     * @param jsonObject 要增加的数据
     * @param index      索引，类似数据库
     * @param type       类型，类似表
     * @param id         ID
     * @return
     */
    @Override
    public void updateDataById(JSONObject jsonObject, String index, String type, String id) {
        index = ElasticsearchUtils.toLowerCase(index);

        UpdateRequest updateRequest = new UpdateRequest();

        updateRequest.index(index).type(type).id(id).doc(jsonObject);

        client.update(updateRequest);
    }

    /**
     * 提升权重
     *
     * @param index       索引，类似数据库
     * @param type        类型，类似表
     * @param id          数据ID
     * @param boostFactor 提升权重
     * @return
     */
    @Override
    public void promptBoostFactor(String index, String type, String id, float boostFactor) {
        index = ElasticsearchUtils.toLowerCase(index);

        UpdateRequest updateRequest = new UpdateRequest();

        Map<String, Float> map = new HashMap<>();
        map.put(SearchDocumentFieldName.BOOSTFACTOR.getFieldName(), boostFactor);

        updateRequest.index(index).type(type).id(id).doc(ElasticsearchUtils.toJson(map));

        client.update(updateRequest);
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
    @Override
    public Corpus searchDataById(String index, String type, String id, String fields) {
        index = ElasticsearchUtils.toLowerCase(index);

        GetRequestBuilder getRequestBuilder = client.prepareGet(index, type, id);

        if (StringUtils.isNotEmpty(fields)) {
            getRequestBuilder.setFetchSource(fields.split(","), null);
        }

        GetResponse getResponse = getRequestBuilder.execute().actionGet();

        return ElasticsearchUtils.toJavaBean(getResponse.getSource(), Corpus.class);
    }

    /**
     * 使用分词查询
     *
     * @param index    索引名称
     * @param type     类型名称,可传入多个type逗号分隔
     * @param fields   需要显示的字段，逗号分隔（缺省为全部字段）
     * @param matchStr 过滤条件（xxx=111,aaa=222）
     * @return
     */
    @Override
    public List<Map<String, Object>> searchListData(String index, String type, String fields, String matchStr) {
        index = ElasticsearchUtils.toLowerCase(index);

        return searchListData(index, type, 0, 0, null, fields, null, false, null, matchStr);
    }

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
    @Override
    public List<Map<String, Object>> searchListData(String index, String type, String fields, String sortField, boolean matchPhrase, String matchStr) {
        index = ElasticsearchUtils.toLowerCase(index);

        return searchListData(index, type, 0, 0, null, fields, sortField, matchPhrase, null, matchStr);
    }

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
    @Override
    public List<Map<String, Object>> searchListData(String index, String type, Integer size, String fields, String sortField, boolean matchPhrase, String highlightField, String matchStr) {
        index = ElasticsearchUtils.toLowerCase(index);

        return searchListData(index, type, 0, 0, size, fields, sortField, matchPhrase, highlightField, matchStr);
    }

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
    @Override
    public List<Map<String, Object>> searchListData(String index, String type, long startTime, long endTime, Integer size, String fields, String sortField, boolean matchPhrase, String highlightField, String matchStr) {
        index = ElasticsearchUtils.toLowerCase(index);

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);

        if (StringUtils.isNotEmpty(type)) {
            searchRequestBuilder.setTypes(type.split(","));
        }
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        if (startTime > 0 && endTime > 0) {
            boolQuery.must(QueryBuilders.rangeQuery("processTime")
                    .format("epoch_millis")
                    .from(startTime)
                    .to(endTime)
                    .includeLower(true)
                    .includeUpper(true));
        }

        //搜索的字段
        if (StringUtils.isNotEmpty(matchStr)) {
            for (String s : matchStr.split(",")) {
                String[] ss = s.split("=");
                if (ss.length > 1) {
                    if (matchPhrase == Boolean.TRUE) {
                        // .slop(1) 短语查询 term 与 term 中间的位数
                        // 比如 搜索 java Lucene
                        // 数据库中的数据 java elasticsearch Lucene
                        // .boost(1) 设置查询权重
                        boolQuery.must(QueryBuilders.matchPhraseQuery(s.split("=")[0], s.split("=")[1]));
                    } else {
                        boolQuery.must(QueryBuilders.matchQuery(s.split("=")[0], s.split("=")[1]));
                    }
                }

            }
        }

        // 高亮（xxx=111,aaa=222）
        if (StringUtils.isNotEmpty(highlightField)) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();

            highlightBuilder.preTags("<span style='color:red' >");//设置前缀
            highlightBuilder.postTags("</span>");//设置后缀

            // 设置高亮字段
            highlightBuilder.field(highlightField);
            searchRequestBuilder.highlighter(highlightBuilder);
        }

        boolQuery.minimumShouldMatch("75%");// 设置匹配率

        searchRequestBuilder.setQuery(boolQuery);

        if (StringUtils.isNotEmpty(fields)) {
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }
        searchRequestBuilder.setFetchSource(true);

        if (StringUtils.isNotEmpty(sortField)) {
            searchRequestBuilder.addSort(sortField, SortOrder.DESC);
        }

        if (size != null && size > 0) {
            searchRequestBuilder.setSize(size);
        }

        //打印的内容 可以在 Elasticsearch head 和 Kibana  上执行查询
        logger.info("\n{}", searchRequestBuilder);

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        long totalHits = searchResponse.getHits().totalHits;
        long length = searchResponse.getHits().getHits().length;

        logger.info("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

        if (searchResponse.status().getStatus() == 200) {
            // 解析对象
            return setSearchResponse(searchResponse, highlightField);
        }
        return null;
    }

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
    @Override
    public EsPage searchDataPage(String index, String type, int currentPage, int pageSize, long startTime, long endTime, String fields, String sortField, boolean matchPhrase, String highlightField, String matchStr) {
        index = ElasticsearchUtils.toLowerCase(index);

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);

        if (StringUtils.isNotEmpty(type)) {
            searchRequestBuilder.setTypes(type.split(","));
        }
        searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);

        // 需要显示的字段，逗号分隔（缺省为全部字段）
        if (StringUtils.isNotEmpty(fields)) {
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }

        //排序字段
        if (StringUtils.isNotEmpty(sortField)) {
            searchRequestBuilder.addSort(sortField, SortOrder.DESC);
        }

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        //boolQuery.must(QueryBuilders.prefixQuery("original", "原文")); // 前缀搜索 但是不会计算 _score 评分

        //boolQuery.must(QueryBuilders.wildcardQuery("","")); // 通配符搜索 尽量不要使用

        //boolQuery.must(QueryBuilders.regexpQuery("", ""));// 正则搜索

        // 搜索推荐 会将最后一个term作为前缀去搜索 销量性能差 使用 maxExpansions 限制最大匹配多少个term
        //boolQuery.must(QueryBuilders.matchPhrasePrefixQuery("", "").maxExpansions(50));

        if (startTime > 0 && endTime > 0) {
            boolQuery.must(QueryBuilders.rangeQuery("processTime")
                    .format("epoch_millis")
                    .from(startTime)
                    .to(endTime)
                    .includeLower(true)
                    .includeUpper(true));
        }

        // 查询字段
        if (StringUtils.isNotEmpty(matchStr)) {
            for (String s : matchStr.split(",")) {
                String[] ss = s.split("=");
                // matchPhraseQuery : 代表匹配短语查询 精确查询
                // matchQuery : 代表匹配查询
                // matchPhrasePrefixQuery : 代表模糊查询
                if (matchPhrase == Boolean.TRUE) {
                    boolQuery.must(QueryBuilders.matchPhraseQuery(s.split("=")[0], s.split("=")[1]));
                } else {
                    boolQuery.must(QueryBuilders.matchQuery(s.split("=")[0], s.split("=")[1]).analyzer("").minimumShouldMatch(""));
                }
            }
        }

        // 高亮（xxx=111,aaa=222）
        if (StringUtils.isNotEmpty(highlightField)) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();

            highlightBuilder.preTags("<span style='color:red' >");//设置前缀
            highlightBuilder.postTags("</span>");//设置后缀

            // 设置高亮字段
            highlightBuilder.field(highlightField);
            searchRequestBuilder.highlighter(highlightBuilder);
        }

        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        searchRequestBuilder.setQuery(boolQuery);

        // 分页应用
        searchRequestBuilder.setFrom(currentPage).setSize(pageSize);

        // 设置是否按查询匹配度排序
        searchRequestBuilder.setExplain(true);

        //打印的内容 可以在 Elasticsearch head 和 Kibana  上执行查询
        logger.info("\n{}", searchRequestBuilder);

        // 执行搜索,返回搜索响应信息
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        long totalHits = searchResponse.getHits().totalHits;
        long length = searchResponse.getHits().getHits().length;

        logger.debug("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

        if (searchResponse.status().getStatus() == 200) {
            // 解析对象
            List<Map<String, Object>> sourceList = setSearchResponse(searchResponse, highlightField);

            return new EsPage(currentPage, pageSize, (int) totalHits, sourceList);
        }
        return null;
    }

    /**
     * 高亮结果集 特殊处理
     *
     * @param searchResponse
     * @param highlightField
     */
    private static List<Map<String, Object>> setSearchResponse(SearchResponse searchResponse, String highlightField) {
        List<Map<String, Object>> sourceList = new ArrayList<Map<String, Object>>();

        StringBuffer stringBuffer = new StringBuffer();

        // 5.5.3 版本 写法
        /*for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            searchHit.getSource().put("id", searchHit.getId());

            if (StringUtils.isNotEmpty(highlightField)) {

                System.out.println("遍历 高亮结果集，覆盖 正常结果集" + searchHit.getSource());
                Text[] text = searchHit.getHighlightFields().get(highlightField).getFragments();

                if (text != null) {
                    for (Text str : text) {
                        stringBuffer.append(str.string());
                    }
                    //遍历 高亮结果集，覆盖 正常结果集
                    searchHit.getSource().put(highlightField, stringBuffer.toString());
                }
            }
            sourceList.add(searchHit.getSource());
        }*/

        // 6.0.0 版本 写法
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            searchHit.getSourceAsMap().put("id", searchHit.getId());

            if (StringUtils.isNotEmpty(highlightField)) {

                System.out.println("遍历 高亮结果集，覆盖 正常结果集" + searchHit.getSourceAsMap());
                Text[] text = searchHit.getHighlightFields().get(highlightField).getFragments();

                if (text != null) {
                    for (Text str : text) {
                        stringBuffer.append(str.string());
                    }
                    //遍历 高亮结果集，覆盖 正常结果集
                    searchHit.getSourceAsMap().put(highlightField, stringBuffer.toString());
                }
            }
            sourceList.add(searchHit.getSourceAsMap());
        }

        return sourceList;
    }

    /**
     * 获取语料库的索引请求生成器
     *
     * @param corpus 数据实体
     * @param index  索引名称
     * @param type   索引类别
     * @return
     * @throws IOException
     */
    private IndexRequestBuilder getIndexRequestBuilderForACorpus(Corpus corpus, String index, String type) throws IOException {
        XContentBuilder contentBuilder = getXContentBuilderForACorpus(corpus);

        IndexRequestBuilder indexRequestBuilder = null;

        if (!TextUtils.isEmpty(corpus.getId())) {
            indexRequestBuilder = client.prepareIndex(index, type, String.valueOf(corpus.getId()));
        } else {
            indexRequestBuilder = client.prepareIndex(index, type);
        }

        indexRequestBuilder.setSource(contentBuilder);

        return indexRequestBuilder;
    }

    /**
     * 获得一个语料库的内容生成器
     *
     * @param corpus 数据实体
     * @return
     * @throws IOException
     */
    private XContentBuilder getXContentBuilderForACorpus(Corpus corpus) throws IOException {
        XContentBuilder contentBuilder = null;
        try {
            contentBuilder = jsonBuilder().prettyPrint().startObject();

            contentBuilder.field(SearchDocumentFieldName.ORIGINAL.getFieldName(), corpus.getOriginal())
                    .field(SearchDocumentFieldName.TRANSLATION.getFieldName(), corpus.getTranslation())
                    .field(SearchDocumentFieldName.FOUNDER.getFieldName(), corpus.getFounder())
                    .field(SearchDocumentFieldName.CREATETIME.getFieldName(), ElasticsearchUtils.formatDate(corpus.getCreateTime()))
                    .field(SearchDocumentFieldName.CHANGETIME.getFieldName(), ElasticsearchUtils.formatDate(corpus.getChangeTime()))
                    .field(SearchDocumentFieldName.DESCRIPTION.getFieldName(), corpus.getDescription())
                    .field(SearchDocumentFieldName.BOOSTFACTOR.getFieldName(), corpus.getBoostFactor());
            //.field(SearchDocumentFieldName.KEYWORDS.getFieldName(), corpus.getKeywords());

            contentBuilder.endObject();
        } catch (IOException ex) {
            logger.error(ex.getMessage());
            throw new RuntimeException("Error occured while creating product gift json document!", ex);
        }

        logger.debug("Generated XContentBuilder for document id {} is {}", new Object[]{corpus.getId(), contentBuilder.prettyPrint().string()});

        return contentBuilder;
    }

    /**
     * 语料库批量请求
     *
     * @param requests
     * @return
     */
    protected BulkResponse corpusBulkRequests(List<IndexRequestBuilder> requests) {
        if (requests.size() > 0) {
            BulkRequestBuilder bulkRequest = client.prepareBulk();

            for (IndexRequestBuilder indexRequestBuilder : requests) {
                bulkRequest.add(indexRequestBuilder);
            }

            logger.debug("Executing bulk index request for size:" + requests.size());

            BulkResponse bulkResponse = bulkRequest.execute().actionGet();

            logger.debug("Bulk operation data index response total items is:" + bulkResponse.getItems().length);
            if (bulkResponse.hasFailures()) {
                // 通过遍历每个批量响应项来处理失败
                logger.error("bulk operation indexing has failures:" + bulkResponse.buildFailureMessage());
            }
            return bulkResponse;
        } else {
            logger.debug("Executing bulk index request for size: 0");
            return null;
        }
    }
}