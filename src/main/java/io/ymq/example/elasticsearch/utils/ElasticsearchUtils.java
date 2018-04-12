/**
 * FileName: ElasticsearchUtils
 * Author:   xiangjunzhong
 * Date:     2017/11/28 16:57
 * Description: Elasticsearch工具类
 */
package io.ymq.example.elasticsearch.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.ymq.example.elasticsearch.model.Corpus;
import org.apache.http.util.TextUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Date;

/**
 * 〈一句话功能简述〉<br>
 * Elasticsearch工具类
 *
 * @author xiangjunzhong
 * @create 2017/11/28 16:57
 * @since 1.0.0
 */
public final class ElasticsearchUtils {

    public static final String SEARCH_DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SSSZZ = "yyyy-MM-dd'T'HH:mm:ssZZ";

    private static final DateTimeFormatter searchDateFormat = ISODateTimeFormat.dateTimeNoMillis();

    //new SimpleDateFormat(SEARCH_DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_SSSZZ);

    public static Date getFormattedDate(String dateString) {
        return searchDateFormat.parseDateTime(dateString).toDate();
    }

    public static String formatDate(Date date) {
        return searchDateFormat.print(new DateTime(date).getMillis());
    }

    /**
     * 将字符串改为小写
     *
     * @param str
     * @return
     */
    public static String toLowerCase(String str) {
        if (!TextUtils.isEmpty(str)) {
            str = str.toLowerCase();
        }
        return str;
    }

    /**
     * 使用 fastjson 定义了一个将对象转化成 JSONObject
     *
     * @param o
     * @return
     */
    public static JSONObject toJson(Object o) {
        return JSONObject.parseObject(JSONObject.toJSONString(o));
    }

    /**
     * 将Elasticsearch返回的数据转换成对象
     *
     * @param o 数据
     * @param c 类型
     * @return
     */
    public static Corpus toJavaBean(Object o, Class c) {
        return (Corpus) JSONObject.toJavaObject((JSON) JSONObject.toJSON(o), c);
    }
}