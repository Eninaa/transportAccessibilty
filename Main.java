import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;


public class Main {
    public static void main(String[] args) throws IOException, ParseException {
        AutoAccessibilty test = new AutoAccessibilty();
        String ConnectionString = "mongodb://192.168.57.102:27017";
        String database = "region63_samarskaya_obl";

        MongoClient mongo = MongoClients.create(ConnectionString);
        MongoDatabase db = mongo.getDatabase(database);

        MongoCollection<Document> houses = db.getCollection("mar_houses");
        MongoCollection<Document> roads = db.getCollection("ta_roads");


        Instant start = Instant.now();



       AggregateIterable h = houses.aggregate(Arrays.asList(new Document("$geoNear",
                new Document("near",
                        new Document("type", "Point")
                                .append("coordinates", Arrays.asList(50.178853123124846d, 53.21259288514409d)))
                        .append("distanceField", "dist")
                        .append("maxDistance", 3000L)
                        .append("spherical", true))));

      //MongoCursor<Document> hc = houses.find().iterator();
      MongoCursor<Document> hc = h.iterator();

      AggregateIterable aggregation = null;

        while (hc.hasNext()) {

            Document d = hc.next();
            ArrayList geo = (ArrayList) d.get("centroid");
            if (geo != null) {
                ObjectId id = (ObjectId) d.get("_id");


               /* aggregation = roads.aggregate(Arrays.asList(new Document("$geoNear",
                                new Document("near",
                                        new Document("type", "Point")
                                                .append("coordinates", Arrays.asList(geo.get(0), geo.get(1))))
                                        .append("distanceField", "dist")
                                        .append("maxDistance", 1500L)
                                        .append("query",
                                                new Document("$or", Arrays.asList(new Document("HIGHWAY", "primary"),
                                                        new Document("HIGHWAY", "primary_link"))))
                                        .append("spherical", true)),
                        new Document("$project",
                                new Document("_id", "$_id")
                                        .append("NAME", "$NAME")
                                        .append("fromHousesToRoads", "$fromHousesToRoads"))));*/

                aggregation = roads.aggregate(Arrays.asList(new Document("$geoNear",
                                new Document("near",
                                        new Document("type", "Point")
                                                .append("coordinates", Arrays.asList(geo.get(0), geo.get(1))))
                                        .append("distanceField", "dist")
                                        .append("maxDistance", 1500L)
                                        .append("spherical", true)),
                        new Document("$project",
                                new Document("_id", "$_id")
                                        .append("NAME", "$NAME")
                                        .append("fromHousesToRoads", "$fromHousesToRoads"))));



                int count = 0;

                MongoCursor<Document> rr = aggregation.iterator();
                while (rr.hasNext()) {
                    Document dc = rr.next();
                    String highway = (String) dc.get("HIGHWAY");
                    if (highway != null) {
                        if (highway.equals("primary") || highway.equals("primary_link")) {
                            ++count;
                        }
                    }
                }
                System.out.println(count);

                BasicDBObject obj = new BasicDBObject();
                obj.put("_id", id);
                JSONObject params = new JSONObject();
                params.put("fromHousesToRoads", count);
                BasicDBObject updateObj = new BasicDBObject();
                updateObj.put("$set", params);
                houses.updateOne(obj, updateObj);
            }
        }
        //Прошло времени, мс: 532782
        //Прошло времени, мс: 564719


        Instant finish = Instant.now();
        long elapsed = Duration.between(start, finish).toMillis();
        System.out.println("Прошло времени, мс: " + elapsed);



    }
}

