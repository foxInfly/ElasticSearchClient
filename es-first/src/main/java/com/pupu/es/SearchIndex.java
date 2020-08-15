package com.pupu.es;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Iterator;

/**
 * @author : lipu
 * @since : 2020-08-15 11:46
 */
public class SearchIndex {

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


    @Test
    public void testSearchById() throws Exception {

        //获取管理员，做索引库的操作，然后指定要创建的索引库的名称,设置后，调用get()执行
//        IdsQueryBuilder builder = QueryBuilders.idsQuery().addIds("1", "2");
        //参数1：要搜索的字段field，参数2是要搜索的关键词
//        TermQueryBuilder builder = QueryBuilders.termQuery("title", "wprd");
        QueryStringQueryBuilder builder = QueryBuilders.queryStringQuery("则文件").defaultField("title");
        SearchResponse response = client.prepareSearch("inex_hello").setTypes("article").setQuery(builder).get();
        SearchHits hits = response.getHits();
        System.out.println("总记录数："+hits.totalHits);
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()){
            SearchHit searchHit = iterator.next();
            System.out.println(searchHit.getSourceAsString());
        }
        client.close();
    }


    @Test
    public void testQueryString() throws Exception {

        QueryStringQueryBuilder builder = QueryBuilders.queryStringQuery("索引库名称")
                .defaultField("title");
        SearchResponse response = client
                .prepareSearch("inex_hello")
                .setTypes("article")
                .setQuery(builder)
                //从哪里开始分页
                .setFrom(0)
                //每页显示的行数
                .setSize(5)
                .get();
        SearchHits hits = response.getHits();
        System.out.println("总记录数："+hits.totalHits);
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()){
            SearchHit searchHit = iterator.next();
            System.out.println(searchHit.getSourceAsString());
        }
        client.close();
    }

    @Test
    public void testSearchhighLight() throws Exception {

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        //高亮显示的字段
        highlightBuilder.field("title");
        highlightBuilder.preTags("<em>");
        highlightBuilder.postTags("</em>");

        QueryStringQueryBuilder builder = QueryBuilders.queryStringQuery("索引库名称")
                .defaultField("title");
        SearchResponse response = client
                .prepareSearch("inex_hello")
                .setTypes("article")
                .setQuery(builder)
                //从哪里开始分页
                .setFrom(0)
                //每页显示的行数
                .highlighter(highlightBuilder)
                .setSize(5)
                .get();
        SearchHits hits = response.getHits();
        System.out.println("总记录数："+hits.totalHits);
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()){
            SearchHit searchHit = iterator.next();
            System.out.println(searchHit.getSourceAsString());
            System.out.println("***********高亮结果**********");
            System.out.println(searchHit.getHighlightFields());
            //取title高亮显示的结果
            HighlightField field = searchHit.getHighlightFields().get("title");
            Text[] fragments = field.getFragments();
            if (fragments != null) {
                String title = fragments[0].toString();
                System.out.println(title);
            }
        }
        client.close();
    }
}
