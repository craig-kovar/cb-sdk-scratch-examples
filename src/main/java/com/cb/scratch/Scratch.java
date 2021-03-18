package com.cb.scratch;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.result.SearchRow;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Scratch {

    public static void main(String[] args) {
        Cluster cluster = Cluster.connect("localhost", "Administrator", "password");
        Bucket bucket = cluster.bucket("demo");
        bucket.waitUntilReady(Duration.ofSeconds(10L));
        Collection collection = bucket.defaultCollection();
        doFtsSearch(cluster, "Third");
    }

    public static void doFtsSearch(Cluster cluster, String term) {
        try {
            SearchResult result = cluster
                    .searchQuery("pages", SearchQuery.queryString(term), SearchOptions.searchOptions().highlight());

            for (SearchRow row : result.rows()) {
                System.out.println("Found row: " + row);
                Map<String,List<String>> map = row.fragments();
                for (String key : map.keySet()) {
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

}
