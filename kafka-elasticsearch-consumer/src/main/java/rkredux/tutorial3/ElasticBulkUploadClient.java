package rkredux.tutorial3;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class ElasticBulkUploadClient {

    public ElasticBulkUploadClient(){
    }

    public static void main(String[] args) throws IOException {
        new ElasticBulkUploadClient().run();
    }

    public void run() throws IOException {
        //build the client
        Logger logger = LoggerFactory.getLogger(ElasticBulkUploadClient.class.getName());
        logger.info("Setting up ES client");
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http"),
                        new HttpHost("localhost", 9202, "http")
                )
        );

        //build bulk request object
        logger.info("Setting up bulk request");
        BulkRequest request = new BulkRequest()
                                .timeout(TimeValue.timeValueMinutes(2))
                                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                                .waitForActiveShards(2);

        //add docs to the bulk request object
        request.add(new IndexRequest("posts").id("13")
                .source(XContentType.JSON,"field", "foo"));
        request.add(new IndexRequest("posts").id("14")
                .source(XContentType.JSON,"field", "bar"));
        request.add(new IndexRequest("posts").id("15")
                .source(XContentType.JSON,"field", "baz"));

        //ship the bulk process
        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);

        //handle the response
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();
            logger.info(String.valueOf(itemResponse));
        }
        //close the es client connection
        logger.info("Closing the client");
        client.close();
    }
}
