package cn.itcast.test;


import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class EsTest {
    private TransportClient transportClient;
    @Before
    public void init() throws UnknownHostException {
        ////创建连接es服务器的客户端(默认配置),需要添加TransportAddress其中参数设置InetSocketTransportAddress，在设置一个通过InetAddress获取的es地址对象
//        InetSocketTransportAddress address = new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300);
//        PreBuiltTransportClient client = new PreBuiltTransportClient(Settings.EMPTY);
//        transportClient = client.addTransportAddress(address);
        InetSocketTransportAddress address = new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300);
        PreBuiltTransportClient client = new PreBuiltTransportClient(Settings.EMPTY);
        transportClient = client.addTransportAddress(address);
    }
    @After
    public void destory(){
        //3.关闭连接   提取到destory方法中
        transportClient.close();
    }
    @Test
    public void creatIndex() throws IOException {
        //构建文档内容  有个article对象属性如下 {"id":1,"title":"这是title","content":"这是content"}
        //通过XContentFactory获取XContentBuilder对象
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
                .field("id",4)
                .field("title","贾文博")
                .field("content","贾文博真菜")
                .endObject();
//        Map map = new HashMap();
//        map.put("id",1);
//        map.put("title","孙方文");
//        map.put("content","孙方文真菜");

        //客户端准备创建索引(索引名称，type名称) 通过setSource设置添加的数据  通过get方法发送http请求restful风格的，将内容发送给es服务器
        //IndexResponse response = transportClient.prepareIndex("blog", "article", "1").setSource(builder).get();
        IndexResponse response = transportClient.prepareIndex("blog", "article", "4").setSource(builder).get();
        System.out.println(response.status());
    }
    @Test
    public void searchIndex(){
        //创建查询对象,matchallquery查询全部
        QueryBuilder allQuery = QueryBuilders.matchAllQuery();
        //通过客户端发送查询请求
        SearchResponse response = transportClient.prepareSearch("blog").setTypes("article").setQuery(allQuery).get();
        //通过response获取查询到的document对象
        SearchHits hits = response.getHits();
        System.out.println("共查询=="+hits.getTotalHits());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }
    @Test
    public void stringQuery(){
        QueryStringQueryBuilder query = QueryBuilders.queryStringQuery("孙");
        SearchResponse response = transportClient.prepareSearch("blog").setTypes("article").setQuery(query).get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }
    @Test
    public void testWildCardSearch(){
        QueryBuilder query = QueryBuilders.wildcardQuery("title", "方*");
        SearchResponse response = transportClient.prepareSearch("blog").setTypes("article").setQuery(query).get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }
    @Test
    public void testTermSearch(){
        QueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", "方");
        SearchResponse response = transportClient.prepareSearch("blog").setTypes("article").setQuery(termQueryBuilder).get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }
}
