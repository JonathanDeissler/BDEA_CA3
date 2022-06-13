import com.couchbase.client.java.*;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.json.*;
import com.couchbase.client.java.query.*;
import java.time.Duration;

public class StartUsing {
    // Update these variables to point to your Couchbase Server instance and credentials.
    static String connectionString = "couchbase://localhost";
    static String username = "Administrator";
    static String password = "123456";
    static String bucketName = "travel-sample";


    public static void main(String... args) {
        Cluster cluster = Cluster.connect(connectionString, username, password);

        // get a bucket reference
        Bucket bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.parse("PT10S")) ;

        // get a user defined collection reference
        Scope scope = bucket.scope("tenant_agent_00");
        Collection collection = scope.collection("users");

        // Upsert Document
        MutationResult upsertResult = collection.upsert(
                "my-document",
                JsonObject.create().put("name", "mike")
        );

        // Get Document
        GetResult getResult = collection.get("my-document");
        String name = getResult.contentAsObject().getString("name");
        System.out.println(name); // name == "mike"

        // Call the query() method on the cluster object and store the result.
        QueryResult result = cluster.query("select \"Hello World\" as greeting");

        // Return the result rows with the rowsAsObject() method and print to the terminal.
        System.out.println(result.rowsAsObject());
    }
}