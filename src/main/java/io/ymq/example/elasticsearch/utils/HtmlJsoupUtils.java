/**
 * FileName: HtmlJsoupUtils
 * Author:   xiangjunzhong
 * Date:     2017/12/19 14:50
 * Description: 解析HTML文件工具类
 */
package io.ymq.example.elasticsearch.utils;

import org.apache.http.util.TextUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * 〈一句话功能简述〉<br>
 * 〈解析HTML文件工具类〉
 *
 * @author xiangjunzhong
 * @create 2017/12/19 14:50
 * @since 1.0.0
 */
public final class HtmlJsoupUtils {

    /**
     * 将 HTML span 标签远程
     *
     * @param url 文件路径
     */
    public static void displaySpan(String url) {
        try {
            String guid = "{af806392-7eea-4cfe-8480-f475eea97a29}:";
            String charsetName = "gb2312";
            StringBuffer qtmPid = null;
            File input = new File(url);
            Document doc = Jsoup.parse(input, charsetName);
            Elements links = doc.select("span[lang]:contains(" + guid + ")");
            if (links != null && links.size() > 0) {
                for (Element link : links) {
                    if (!TextUtils.isEmpty(link.text())) {
                        qtmPid = new StringBuffer("qtmPid-");
                        qtmPid.append(link.text().substring(guid.length(), link.text().length()));
                        link.attr("id", qtmPid.toString());
                        link.attr("style", "display: none;");
                    }
                }
                // Jsoup只是解析，不能保存修改，所以要在这里保存修改
                FileOutputStream fos = new FileOutputStream(input, false);
                OutputStreamWriter osw = new OutputStreamWriter(fos, charsetName);
                osw.write(doc.html());
                osw.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}