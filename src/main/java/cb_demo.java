import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.subdoc.MultiMutationException;


public class cb_demo {

    public static void main(String[] args) {

        String endpoint = "localhost";
        String username = "Administrator";
        String password = "password";
        String bucketName = "test";


       /* CouchbaseEnvironment env = DefaultCouchbaseEnvironment
                .builder()
                .mutationTokensEnabled(true)
                .computationPoolSize(5)
                .kvTimeout(10L)
                .build();
        Cluster cluster = CouchbaseCluster.create(env, "localhost"); */

        Cluster cluster = CouchbaseCluster.create(endpoint);
        cluster.authenticate(username, password);
        Bucket bucket = cluster.openBucket(bucketName);

        //bucket.remove("u:king_arthur");

        /*JsonObject arthur = JsonObject.create()
                .put("name", "Arthur")
                .put("email", "kingarthur@couchbase.com")
                .put("ids", JsonArray.create());

        bucket.upsert(JsonDocument.create("u:king_arthur", arthur)); */

        try {
            bucket.mutateIn("u:king_arthur")
                    .arrayAddUnique("ids", "tid4")
                    .arrayAddUnique("ids", "id2")
                    .arrayAddUnique("ids", "id3")
                    .execute();
        } catch (MultiMutationException err) {
            System.out.println("arrayUnique: caught exception, value already exists");
            String msg = err.getMessage().split("First problematic failure at ")[1];
            String index = msg.substring(0,msg.indexOf(" with status"));
            System.out.println("Index = " + index);
        }

        bucket.close();
        cluster.disconnect();


    }

}
