
import com.mongodb.client.*;

import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;

public class Main {

    public static void main(String[] args) throws FileNotFoundException{

        //writeData();

        readData();

    }

    public static void writeData() throws FileNotFoundException{
        //connect
        MongoClient mongoClient = MongoClients.create();
        MongoDatabase database = mongoClient.getDatabase("testDB");
        MongoCollection<Document> collection = database.getCollection("testPoints");


        //file scanner
        Scanner scanner = new Scanner(new File("/home/livia/Data/swath_20110402-054158_20110402-054158_0.csv"));
        scanner.useDelimiter(",");
        String[] header = scanner.nextLine().split(",");
        List<Document> documents = new ArrayList<Document>();
        while(scanner.hasNextLine()){
            Document doc = new Document();
            String[] line = scanner.nextLine().split(",");
            for(int i = 1; i<header.length; i++){
                doc.append(header[i], line[i]);
            }
            documents.add(doc);
        }
        collection.insertMany(documents);
        scanner.close();
    }


    public static void readData(){
        //connect
        MongoClient mongoClient = MongoClients.create();
        MongoDatabase database = mongoClient.getDatabase("testDB");
        MongoCollection<Document> collection = database.getCollection("testPoints");


        //count documents
        System.out.println(collection.countDocuments());

        //get first
        Document myDoc = collection.find().first();
        System.out.println(myDoc.toJson());

        //WHERE equals
        myDoc = collection.find(eq("coh", "0.649")).first();
        System.out.println(myDoc.toJson());


        // multiple with greater than
        MongoCursor<Document> cursor = collection.find(gt("coh", "0.998")).iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }

    }

}
