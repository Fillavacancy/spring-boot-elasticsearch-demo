/**
 * FileName: JsonVo
 * Author:   xiangjunzhong
 * Date:     2017/11/29 9:47
 * Description: Controller 数据传输对象
 */
package io.ymq.example.elasticsearch.model;

/**
 * 〈一句话功能简述〉<br>
 * 〈Controller 数据传输对象〉
 *
 * @author xiangjunzhong
 * @create 2017/11/29 9:47
 * @since 1.0.0
 */
public class JsonVo {

    /**
     * 消息
     */
    private String msg;

    /**
     * 状态
     */
    private byte status;

    /**
     * 数据
     */
    private Object data;

    public JsonVo() {

    }

    public JsonVo(byte status) {
        this.status = status;
    }

    public JsonVo(String msg, byte status) {
        this.msg = msg;
        this.status = status;
    }

    public JsonVo(String msg, byte status, Object data) {
        this.msg = msg;
        this.status = status;
        this.data = data;
    }

    public JsonVo(byte status, Object data) {
        this.status = status;
        this.data = data;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public byte getStatus() {
        return status;
    }

    public void setStatus(byte status) {
        this.status = status;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}