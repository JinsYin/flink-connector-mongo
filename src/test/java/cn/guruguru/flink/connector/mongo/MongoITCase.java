package cn.guruguru.flink.connector.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static org.junit.Assert.assertEquals;

public class MongoITCase extends MongoTestingClusterAutoStarter {

    private final String TEST_MG_DATABASE = "testDatabase";
    private final String TEST_MG_COLLECTION = "testCollection";

    // ~ insert, upsert and query ----------------------------------------------

    @Test
    public void testInsertOneAndReplaceOne() {
        MongoCollection<BsonDocument> mgCollection = getTestMongoClient()
                .getDatabase(TEST_MG_DATABASE)
                .getCollection(TEST_MG_COLLECTION, BsonDocument.class);

        BsonObjectId objectId = new BsonObjectId();
        BsonDocument origin = new BsonDocument()
                .append("_id", objectId)
                .append("k1", new BsonString("value"))
                .append("k2", new BsonInt32(100));

        BsonDocument replacement = new BsonDocument()
                //.append("_id", new BsonObjectId()) // ObjectId is imutable
                .append("x", new BsonInt32(100))
                .append("y", new BsonInt32(200));
        BsonDocument filter = new BsonDocument()
                .append("k1", new BsonString("value"));
        BsonDocument expectedReplacement = replacement.append("_id", objectId);

        // Compare numbers before applying a operation
        assertEquals(0, mgCollection.countDocuments());

        // insert
        mgCollection.insertOne(origin);

        // Compare numbers and results after insert a document
        assertEquals(1, mgCollection.countDocuments());
        assertEquals(origin, mgCollection.find().first()); // findOne

        // replace
        mgCollection.replaceOne(filter, replacement);

        // Compare numbers and results after replace a document
        assertEquals(1, mgCollection.countDocuments());
        assertEquals(expectedReplacement, mgCollection.find().first());
    }

    @Test
    public void testInsertMany() {
        MongoCollection<BsonDocument> mgCollection = getTestMongoClient()
                .getDatabase(TEST_MG_DATABASE)
                .getCollection(TEST_MG_COLLECTION, BsonDocument.class);

        assertEquals(0, mgCollection.countDocuments());

        List<BsonDocument> documents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            documents.add(new BsonDocument("i", new BsonInt32(i)));
        }
        mgCollection.insertMany(documents);

        List<BsonDocument> results = new ArrayList<>();
        mgCollection.find().forEach(results::add); // findAll

        // Compare numbers and results
        assertEquals(100, mgCollection.countDocuments());
        assertEquals(documents, results);
    }

    // ~ projections -----------------------------------------------

    @Test
    public void testProjection() {
        List<BsonDocument> documents = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            BsonDocument bsonDocument = new BsonDocument();
            bsonDocument.append("name", new BsonInt32(i));
            bsonDocument.append("stars", new BsonInt32(i * 10));
            bsonDocument.append("categories", new BsonInt32(i * 100));
            bsonDocument.append("tags", new BsonInt32(i * 1000));
            documents.add(bsonDocument);
        }
        getTestMongoCollection().insertMany(documents);

        // method 1
        List<BsonDocument> resultSet1 = new ArrayList<>();
        getTestMongoCollection().find()
                .projection(
                    new BsonDocument("name", new BsonInt32(1))
                        .append("stars", new BsonInt32(1))
                        .append("categories",new BsonInt32(1))
                        .append("_id", new BsonInt32(0))) // no _id field
                .forEach(resultSet1::add);

        // method 2
        List<BsonDocument> resultSet2 = new ArrayList<>();
        getTestMongoCollection().find()
                .projection(fields(include("name", "stars", "categories"), exclude("_id")))
                .forEach(resultSet2::add);

        List<BsonDocument> expected = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            BsonDocument bsonDocument = new BsonDocument();
            bsonDocument.append("name", new BsonInt32(i));
            bsonDocument.append("stars", new BsonInt32(i * 10));
            bsonDocument.append("categories", new BsonInt32(i * 100));
            expected.add(bsonDocument);
        }

        assertEquals(resultSet1, resultSet2);
        assertEquals(expected, resultSet1);
    }

    // ~ filters --------------------------------------------

    @Test
    public void testFindOneByFilter() {
        List<BsonDocument> documents = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            documents.add(new BsonDocument("i" + i, new BsonInt32(i)));
        }
        getTestMongoCollection().insertMany(documents);

        // method 1
        BsonDocument result1 = getTestMongoCollection().find()
                .filter(eq("i7", new BsonInt32(7)))
                .projection(exclude("_id"))
                .first();

        // method 2
        BsonDocument result2 = getTestMongoCollection()
                .find(eq("i7", new BsonInt32(7)))
                .projection(exclude("_id"))
                .first();

        // method 3
        BsonDocument result3 = getTestMongoCollection()
                .find(new BsonDocument("i7", new BsonInt32(7)))
                .projection(exclude("_id"))
                .first();

        BsonDocument expected = new BsonDocument("i7", new BsonInt32(7));

        assertEquals(result1, result2);
        assertEquals(result1, result3);
        assertEquals(expected, result1);
    }

    @Test
    public void testFindAllByFilter() {
        List<BsonDocument> documents = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            BsonDocument document = new BsonDocument()
                    .append("x" + i / 2, new BsonInt32(i / 2))
                    .append("other", new BsonString(String.valueOf(i)));
            documents.add(document);
        }
        getTestMongoCollection().insertMany(documents);

        List<BsonDocument> resultSet = new ArrayList<>();
        getTestMongoCollection().find()
                .filter(eq("x4", 4))
                .projection(exclude("_id"))
                .forEach(resultSet::add);

        List<BsonDocument> expected = new ArrayList<>();
        BsonDocument document1 = new BsonDocument()
                .append("x4", new BsonInt32(4))
                .append("other", new BsonString("8"));
        BsonDocument document2 = new BsonDocument()
                .append("x4", new BsonInt32(4))
                .append("other", new BsonString("9"));
        expected.add(document1);
        expected.add(document2);

        assertEquals(2, resultSet.size());
        assertEquals(expected, resultSet);
    }

    @Ignore
    public void testUpdateOne() {
        getTestMongoCollection().updateOne(eq("i", 10), set("i", 20));
    }

    @Ignore
    public void testUpdateMany() {
        UpdateResult updateResult = getTestMongoCollection().updateMany(eq("a", 1), inc("a", 10));
        System.out.println(updateResult.getMatchedCount());
        System.out.println(updateResult.getModifiedCount());
    }

    @Ignore
    public void testDeleteOne() {
        DeleteResult deleteResult = getTestMongoCollection().deleteOne(eq("a", 1));
        System.out.println(deleteResult.getDeletedCount());
    }

    @Ignore
    public void testDeleteMany() {
        DeleteResult deleteResult = getTestMongoCollection().deleteMany(eq("a", 1));
        System.out.println(deleteResult.getDeletedCount());
    }
}
