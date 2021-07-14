package com.cb.scratch;

import com.couchbase.client.core.env.*;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.queries.BooleanQuery;
import com.couchbase.client.java.search.queries.MatchQuery;
import com.couchbase.client.java.search.queries.TermQuery;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.result.SearchRow;
import com.couchbase.client.java.search.result.SearchRowLocations;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;

public class Scratch {

    public static void main(String[] args) {

        ThresholdRequestTracerConfig.Builder config = ThresholdRequestTracerConfig.builder()
                .emitInterval(Duration.ofSeconds(600))
                .kvThreshold(Duration.ofMillis(1))
                .queryThreshold(Duration.ofSeconds(1))
                .searchThreshold(Duration.ofSeconds(1))
                .analyticsThreshold(Duration.ofSeconds(1))
                .sampleSize(1); //Default 10

        OrphanReporterConfig.Builder orphCfg = OrphanReporterConfig.builder()
                .emitInterval(Duration.ofSeconds(10))
                .sampleSize(10)
                .enabled(true);

        ClusterEnvironment environment = ClusterEnvironment.builder()
                .thresholdRequestTracerConfig(config)
                .orphanReporterConfig(orphCfg)
                //.timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofMillis(3)))
                //Developer Preview
                .aggregatingMeterConfig(AggregatingMeterConfig.enabled(true)
                        .emitInterval(Duration.ofSeconds(10))) //Default 10 Minutes
                .build();

        Cluster cluster = Cluster.connect("localhost", ClusterOptions
                .clusterOptions("Administrator", "password")
                .environment(environment)
        );
        Bucket bucket = cluster.bucket("travel-sample");
        bucket.waitUntilReady(Duration.ofSeconds(10L));
        Collection collection = bucket.defaultCollection();

        for (int i =0; i< 20000; i++) {
            try {
                GetResult result = collection.get("airline_10");
            } catch (CouchbaseException ex) {
                //Suppress all Couchbase Exceptions
                //throw ex;
            }
        }

        //doFtsSearch(cluster, "ishmael");
        //doN1QLSearch(cluster);
        //doFTSN1qlSearch(cluster);

        //Graceful Shut down
        cluster.disconnect();
        environment.shutdown();
    }

    public static void doFtsSearch(Cluster cluster, String term) {
        try {

            BooleanQuery booleanQuery = new BooleanQuery();
            MatchQuery matchQuery = SearchQuery.match(term);
            //TermQuery modNum = SearchQuery.term("None").field("mod_num");
            //TermQuery orderNum = SearchQuery.term("None").field("order_num");

            booleanQuery.must(matchQuery);
            //System.out.println(booleanQuery.export());

            SearchResult result = cluster
                    .searchQuery("page", booleanQuery, SearchOptions.searchOptions().highlight());

            for (SearchRow row : result.rows()) {
                System.out.println("Found row: " + row);
                Map<String,List<String>> map = row.fragments();
                TreeMap<String,List<String>> sorted = new TreeMap<>(map);
                for (String key : sorted.keySet()) {
                    System.out.println("Found on page " + key);
                    List<String> fragments = map.get(key);
                    for (String itr : fragments) {
                        System.out.println("Fragment = " + itr);
                    }
                }
            }
        } catch (CouchbaseException ex) {
            ex.printStackTrace();
        }
    }

    public static void doN1QLSearch(Cluster cluster) {
        try {
            try {
                final QueryResult result = cluster.query("select * from `travel-sample`",
                        queryOptions().metrics(false).adhoc(false).clientContextId("mynewid"));

                for (JsonObject row : result.rowsAsObject()) {
                    System.out.println("Found row: " + row);
                }

                //System.out.println("Reported execution time: " + result.metaData().metrics());
            } catch (CouchbaseException ex) {
                ex.printStackTrace();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void doFTSN1qlSearch(Cluster cluster) {
         String query = "WITH distinctFts AS (\n" +
                 "    WITH ftsSearch AS (\n" +
                 "        SELECT \"contract:\"||SPLIT(SEARCH_META().id,\":\")[1] AS contract,\n" +
                 "               SEARCH_META().locations AS locations,\n" +
                 "               SEARCH_META().fields AS fields\n" +
                 "        FROM demo d USE INDEX (test USING FTS)\n" +
                 "        WHERE SEARCH(d,{\"highlight\": {}, \"fields\": [\"*\"], \"query\" : {\"match\":\"%s\"}})\n" +
                 "            AND doc_type = \"PAGE\" \n" +
                 "    )\n" +
                 "    \n" +
                 "    SELECT ftsS.contract as contract, \n" +
                 "           ARRAY_AGG(ftsS.locations) as locations, \n" +
                 "           ARRAY_AGG(ftsS.fields) as fields\n" +
                 "    FROM ftsSearch ftsS group by ftsS.contract\n" +
                 ")\n" +
                 "            \n" +
                 "SELECT d.*,fs.* \n" +
                 "FROM distinctFts fs\n" +
                 "JOIN demo d ON KEYS [ fs.contract ];";

        try {
            final QueryResult result = cluster.query(String.format(query,"ishmael"),
                    queryOptions().metrics(false).adhoc(false));

            for (JsonObject row : result.rowsAsObject()) {
                JsonArray ja = row.getArray("locations");
                for (int i = 0; i<ja.size(); i++) {
                    convertAndPrintSearchRowLocations(ja.getObject(i));
                }
            }
        } catch (CouchbaseException ex) {
            ex.printStackTrace();
        }
    }

    public static void convertAndPrintSearchRowLocations(JsonObject jo) {
        SearchRowLocations srl = SearchRowLocations.from(jo);
        System.out.println(srl.toString());
    }

}
