package com.kaicom.test;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

public class EsTest {
	
	private final String INDEX = "crm";
	private final String TYPE = "table";
	//@Test
	public TransportClient getClient() throws Exception{
		//创建配置对象
		Settings settings = Settings.builder().put("client.transport.sniff", true).build();
		//创建传输客户端
		TransportClient client = new PreBuiltTransportClient(settings);
		//还要线索
		TransportAddress transportAddress = new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300);
		client.addTransportAddress(transportAddress);
		System.out.println(client);
		//client.close();
		return client ;
	}
	
	//创建索引
	@Test
	public void testCreate() throws Exception{
		TransportClient client = getClient();
		for(int i = 1 ; i <= 50 ; i++){
			//执行操作
			//1)创建执行对象
			IndexRequestBuilder prepareIndex = client.prepareIndex(INDEX, TYPE, ""+i);
			//2)准备执行对象所需的数据
			Map<String,Object> source = new HashMap<>();
			source.put("id", i);
			source.put("name", "el"+i);
			source.put("age", 18+i);
			//将数据添加给执行对象
			IndexRequestBuilder builder = prepareIndex.setSource(source);
			//发送数据
			System.out.println(builder.get());
		}
		client.close();
	}
	
	//获取
	@Test
	public void testGet() throws Exception{
		TransportClient client = getClient();
		//创建执行对象
		GetRequestBuilder prepareGet = client.prepareGet(INDEX, TYPE, "1");
		GetResponse getResponse = prepareGet.get();
		System.out.println(getResponse.getSource());
		client.close();
	}
	
	//修改(局部更新)
	@Test
	public void testUpdate() throws Exception{
		TransportClient client = getClient();
		UpdateRequestBuilder prepareUpdate = client.prepareUpdate(INDEX, TYPE, "1");
		Map<String,Object> source = new HashMap<>();
		source.put("id", 1);
		source.put("name", "el-edit");
		prepareUpdate.setDoc(source).get();
		client.close();
	}
	
	//删除
	@Test
	public void testDel() throws Exception{
		TransportClient client = getClient();
		DeleteRequestBuilder builder = client.prepareDelete(INDEX, TYPE, "1");
		builder.get();
		client.close();
	}
	
	//dls
	/**
	 * #dsl过滤
		GET test/employee/_search
		{
		  "query": {
		    "match": {
		      "name": "xuhanghang"
		    },
		    "bool": {
		      "filter": {
		        "term": {
		          "FIELD": "VALUE"
		        }
		      }
		    }
		  },
		  "sort": [
		    {
		      "age": {
		        "order": "desc"
		      }
		    }
		  ],
		  "from": 0,
		  "size": 2,
		  "_source": ["name","age"]
		}
	 */
	@Test
	public void testDls() throws Exception{
		TransportClient client = getClient();
		SearchRequestBuilder builder = client.prepareSearch(INDEX);
		builder.setTypes(TYPE);
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
		boolQuery.must(QueryBuilders.matchAllQuery());
		boolQuery.filter(QueryBuilders.rangeQuery("age").gte(30).lte(50));
		builder.setQuery(boolQuery);
		builder.addSort("age", SortOrder.DESC);
		builder.setFrom(10).setSize(10);
		builder.setFetchSource(new String[]{"id","age"}, null);
		SearchResponse response = builder.get();
		SearchHits hits = response.getHits();
		System.out.println("总命中数量："+hits.getTotalHits());
		SearchHit[] hits2 = hits.getHits();
		for (SearchHit searchHit : hits2) {
			System.out.println(searchHit.getSource());
		}
		client.close();
	}
}
