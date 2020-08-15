package com.pupu.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

/**
 * @author : lipu
 * @since : 2020-08-14 22:09
 */
public class ElasticSearchClientTest {

    private PreBuiltTransportClient client;

    @Before
    public void init()throws Exception{
        Settings settings = Settings.builder()
                .put("cluster.name", "my-elasticsearch")
                .build();

        client = new PreBuiltTransportClient(settings);
        //第一个参数根据ip获取名称;这里是停车票连接对应端口是9300
        System.out.println(InetAddress.getByName("127.0.0.1"));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9303));
    }


    /**
     * 1.2创建索引库（不设置mappings）
     *      1、创建一个Settings对象，相当于是一配置信息。主要配置集群的名称。确定要连接那个集群
     *      2、创建一个客户端Client对象
     *      3、使用client对象创建一 个索引库
     *      4、关闭client对象
     *
     * @author lipu
     * @since 2020/8/14 22:09
     */
    @Test
    public void createIndex() throws Exception {

        //获取管理员，做索引库的操作，然后指定要创建的索引库的名称,设置后，调用get()执行
        client.admin().indices().prepareCreate("inex_hello").get();
        client.close();
    }

    /**
     *  1.3给index添加mappings:
     *         1)创建一个Settings对 象
     *         2)创建一个Client对象
     *         3)创建一个mapping信息，应该是一个json数据，可以是字符串，也可以是XContextBuilder对象
     *         4)使用client向es服务器发送mapping信息
     *         5)关闭client对象
     *
     * @author lipu
     * @since 2020/8/14 22:26
     */
    @Test
    public void setMappings() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("article")
                        .startObject("properties")
                            .startObject("id")
                                .field("type","long")
                                .field("store",true)
                            .endObject()
                            .startObject("title")
                                .field("type","text")
                                .field("store",true)
                                .field("analyzer","ik_smart")
                            .endObject()
                            .startObject("content")
                                .field("type","text")
                                .field("store",true)
                                .field("analyzer","ik_smart")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        client.admin().indices().preparePutMapping("inex_hello").setType("article").setSource(builder).get();
        client.close();
    }


    @Test
    public void testAddDocument() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .field("id",3)
                    .field("title","word文件编辑后关闭未保存")
                    .field("content","如果是正常未保存是找不回来了，如果是特殊情况下的强制关闭，则文件还能找回来")
                .endObject();
        //获取管理员，做索引库的操作，然后指定要创建的索引库的名称,设置后，调用get()执行
        client.prepareIndex("inex_hello","article","3").setSource(builder).get();
        client.close();
    }


    @Test
    public void testAddDocument2() throws Exception {
        Article article = new Article();
        article.setId(4L);
        article.setTitle("4444444444");
        article.setContent("4444444444");

        ObjectMapper objectMapper = new ObjectMapper();
        String jsonDocument = objectMapper.writeValueAsString(article);
        System.out.println(jsonDocument);

        //获取管理员，做索引库的操作，然后指定要创建的索引库的名称,设置后，调用get()执行
        client.prepareIndex("inex_hello","article","4").setSource(jsonDocument, XContentType.JSON).get();
        client.close();
    }


    @Test
    public void testAddDocument3() throws Exception {
        for (int i = 5; i < 100; i++) {
            Article article = new Article();
            article.setId((long) i);
            article.setTitle("做索引库的操作"+i);
            article.setContent("然后指定要创建的索引库的名称"+i);

            ObjectMapper objectMapper = new ObjectMapper();
            String jsonDocument = objectMapper.writeValueAsString(article);
            System.out.println(jsonDocument);

            //获取管理员，做索引库的操作，然后指定要创建的索引库的名称,设置后，调用get()执行
            client.prepareIndex("inex_hello","article",i+"")
                    .setSource(jsonDocument, XContentType.JSON).get();
        }

        client.close();
    }
}
