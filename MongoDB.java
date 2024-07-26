import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;

import com.mongodb.client.model.Accumulators; 

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.FindIterable;
import static com.mongodb.client.model.Sorts.*;
import com.mongodb.client.model.Sorts;
import static com.mongodb.client.model.Aggregates.*;

/**
 * Program to create a collection, insert JSON objects, and perform simple
 * queries on MongoDB.
 */
public class MongoDB {
    /**
     * MongoDB database name
     */
    public static final String DATABASE_NAME = "mydb";
    /**
     * MongoDB collection name
     */
    public static final String COLLECTION_NAME = "data";
    /**
     * Mongo client connection to server
     */
    public MongoClient mongoClient;
    /**
     * Mongo database
     */
    public MongoDatabase db;
    /**
	* Main method
	* 
	* @param args
	*             
	no arguments required
	*/
    public static void main(String[] args) throws Exception {
        MongoDB qmongo = new MongoDB();
        qmongo.connect();
        qmongo.load();
        qmongo.loadNest();
        System.out.println(qmongo.query1(1000));
        System.out.println(qmongo.query2(32));
        System.out.println(qmongo.query2Nest(32));
        System.out.println(qmongo.query3());
        System.out.println(qmongo.query3Nest());
        System.out.println(MongoDB.toString(qmongo.query4()));
        System.out.println(MongoDB.toString(qmongo.query4Nest()));
    }
    /**
	* Connects to Mongo database and returns database object to manipulate for
	* connection.
	* 
	* @return
	*         
	Mongo database
	*/
    public MongoDatabase connect() {
        try 
		{
            // Provide connection information to MongoDB server
            // TODO: Replace with your cluster info
			System.out.println("\n\n\nConnecting to MongoDB...");
            String url = "mongodb+srv://g23ai1010:g23ai1010@clusterbdm.kldrtxv.mongodb.net/?retryWrites=true&w=majority&appName=ClusterBDM";
            mongoClient = MongoClients.create(url);
			System.out.println("\n\n\n\nCONNEFCTION SUCCESSFUL." );
        } 
		catch (Exception ex) 
		{
            System.out.println("Exception: " + ex);
            ex.printStackTrace();
        }
        // Provide database information to connect to
        // Note: If the database does not already exist, it will be created
        // automatically.
        db = mongoClient.getDatabase(DATABASE_NAME);
        return db;
    }
    ///*** Loads TPC-H data into MongoDB.
	//** @throws Exception
	//*if a file I/O or database error occurs
	//*/
    public void load() throws Exception 
	{
        // TODO: Load customer and orders data
		MongoCollection<Document> customer_col = db.getCollection("customer");
        MongoCollection<Document> orders_col = db.getCollection("orders");

        try (BufferedReader customerReader = new BufferedReader(new FileReader("data/customer.tbl"))) 
		{
			System.out.println("\n\nData loading into Customer Collection...");
            String line;
            while ((line = customerReader.readLine()) != null) {
                String[] fields = line.split("\\|");
                Document cust = new Document()
                    .append("custkey", Integer.parseInt(fields[0]))
                    .append("name", fields[1])
                    .append("address", fields[2])
                    .append("nationkey", Integer.parseInt(fields[3]))
                    .append("phone", fields[4])
                    .append("acctbal", new BigDecimal(fields[5]))
                    .append("mktsegment", fields[6])
                    .append("comment", fields[7]);
                customer_col.insertOne(cust);
            }
			System.out.println("\n\nData loaded Successfully into Customer Collection.");
        }

        try (BufferedReader ordersReader = new BufferedReader(new FileReader("data/order.tbl"))) 
		{
			System.out.println("\n\nData loading into Order Collection...");
            String line;
            while ((line = ordersReader.readLine()) != null) {
                String[] fields = line.split("\\|");
                Document ord = new Document()
                    .append("orderkey", Integer.parseInt(fields[0]))
                    .append("custkey", Integer.parseInt(fields[1]))
                    .append("orderstatus", fields[2])
                    .append("totalprice", new BigDecimal(fields[3]))
                    .append("orderdate", fields[4])
                    .append("orderpriority", fields[5])
                    .append("clerk", fields[6])
                    .append("shippriority", Integer.parseInt(fields[7]))
                    .append("comment", fields[8]);
                orders_col.insertOne(ord);
            }
			System.out.println("\n\nData loaded Successfully into Order Collection.");
		}
    }
    ///**
	//* Loads customer and orders TPC-H data into a single collection.
	//* 
	//* @throws Exception
	//*                   
	//if 	a file I/O or database error occurs
	//*/
    public void loadNest() throws Exception 
	{
		System.out.println("\n\nData Loading into Custorder Collection...");
		
        // TODO: Load customer and orders data into single collection called custorders
        // TODO: Consider using insertMany() for bulk insert for faster performance
		MongoCollection<Document> custorders = db.getCollection("custorders");

		// Create a map to store customer data with their corresponding orders
		Map<Integer, Document> customer_map = new HashMap<>();
	
		// Read customer data
		try (BufferedReader customerbr = new BufferedReader(new FileReader("data/customer.tbl"))) 
		{
			String line;
			while ((line = customerbr.readLine()) != null) 
			{
				String[] fields = line.split("\\|");
				Document customer = new Document()
					.append("custkey", Integer.parseInt(fields[0]))
					.append("name", fields[1])
					.append("address", fields[2])
					.append("nationkey", Integer.parseInt(fields[3]))
					.append("phone", fields[4])
					.append("acctbal", new BigDecimal(fields[5]))
					.append("mktsegment", fields[6])
					.append("comment", fields[7])
					.append("orders", new ArrayList<Document>());
				customer_map.put(Integer.parseInt(fields[0]), customer);
			}
		}
	
		// Read order data and associate each order with the corresponding customer
		try (BufferedReader ordersbr = new BufferedReader(new FileReader("data/order.tbl"))) 
		{
			String line;
			while ((line = ordersbr.readLine()) != null) 
			{
				String[] fields = line.split("\\|");
				Document order = new Document()
					.append("orderkey", Integer.parseInt(fields[0]))
					.append("orderstatus", fields[2])
					.append("totalprice", new BigDecimal(fields[3]))
					.append("orderdate", fields[4])
					.append("orderpriority", fields[5])
					.append("clerk", fields[6])
					.append("shippriority", Integer.parseInt(fields[7]))
					.append("comment", fields[8]);
	
				int custkey = Integer.parseInt(fields[1]);
				if (customer_map.containsKey(custkey)) 
				{
					customer_map.get(custkey).getList("orders", Document.class).add(order);
				}
			}
		}
	
		// Insert the nested customer documents into the custorders collection
		custorders.insertMany(new ArrayList<>(customer_map.values()));
		System.out.println("\n\nData Loaded Successfully in Custorder Collection.");
    }
	/**
	* Performs a MongoDB query that prints out all data (except for the _id).
     */
    public String query1(int custkey) {
		// TODO: Write query
		
        System.out.println("\n\nExecuting query 1:");
		MongoCollection < Document > col = db.getCollection("customer");
			
		Document cust = col.find(eq("custkey", custkey)).projection(fields(include("name"), excludeId())).first();
		return cust != null ? cust.getString("name") : "No customer found with custkey: " + custkey;
    }
    /**
     * Performs a MongoDB query that returns order date for a given order id using
     * the orders collection.
     */
    public String query2(int orderId) {
        // TODO: Write a MongoDB query
        System.out.println("\nExecuting query 2:");
		MongoCollection < Document > col = db.getCollection("orders");
		Document order = col.find(eq("orderkey", orderId)).projection(fields(include("orderdate"),excludeId())).first();
        return order != null ? order.getString("orderdate") : "No order found with Orderid: " + orderId;
    }
    /**
     * Performs a MongoDB query that returns order date for a given order id using
     * the custorders collection.
     */
    public String query2Nest(int orderId) {
        // TODO: Write a MongoDB query
        System.out.println("\nExecuting query 2 nested:");
        MongoCollection < Document > col = db.getCollection("custorders");
        Document res = col.find(eq("orders.orderkey", orderId)).projection(fields(include("orders"),excludeId())).first();
        if (res != null && res.containsKey("orders")) {
        List<Document> orders = (List<Document>) res.get("orders");
        if (!orders.isEmpty()) {
            return orders.get(0).getString("orderdate");
        }
    }
    return "No order found with orderkey: " + orderId;
    }
    /**
     * Performs a MongoDB query that returns the total number of orders using the
     * orders collection.
     */
    public long query3() {
        // TODO: Write a MongoDB query
        System.out.println("\nExecuting query 3:");
        MongoCollection < Document > col = db.getCollection("orders");
		long cnt = col.countDocuments();
        return cnt;
    }
    /**
     * Performs a MongoDB query that returns the total number of orders using the
     * custorders collection.
     */
    public long query3Nest() 
	{
        // TODO: Write a MongoDB query
        System.out.println("\nExecuting query 3 nested:");
        MongoCollection < Document > col = db.getCollection("custorders");
		// Aggregation pipeline to count total number of orders
		// Aggregation pipeline to count total number of orders
		AggregateIterable<Document> res = col.aggregate(Arrays.asList(
				Aggregates.unwind("$orders"),
				Aggregates.count("totalOrders")
		));
	
		Document doc = res.first();
		long cnt = doc != null ? doc.getInteger("totalOrders", 0) : 0;
	
		return cnt;
		
    }
    /**
     * Performs a MongoDB query that returns the top 5 customers based on total
     * order amount using the customer and orders collections.
     */
    public MongoCursor < Document > query4() 
	{
        // TODO: Write a MongoDB query. Note: Return an iterator.
        System.out.println("\nExecuting query 4:");
		MongoCollection<Document> customer_col = db.getCollection("customer");
		//MongoCollection<Document> ordersCollection = db.getCollection("orders");
		
        // Aggregate pipeline to calculate total order amount per customer and sort by total amount
        AggregateIterable<Document> res = customer_col.aggregate(Arrays.asList(
                // Lookup orders for each customer
                Aggregates.lookup("orders", "custkey", "custkey", "orders"),
                // Unwind orders array
                Aggregates.unwind("$orders"),
                // Group by customer and calculate total order amount
                Aggregates.group("$name",
                        Accumulators.sum("totalOrders", "$orders.totalprice")),
                // Sort by total order amount descending
                Aggregates.sort(Sorts.descending("totalOrders")),
                // Limit to top 5 customers
                Aggregates.limit(5)
        ));

        return res.iterator();
	}
    /**
     * Performs a MongoDB query that returns the top 5 customers based on total
     * order amount using the custorders collection.
     */
    public MongoCursor < Document > query4Nest() {
        // TODO: Write a MongoDB query. Note: Return an iterator.
        System.out.println("\nExecuting query 4 nested:");
        MongoCollection < Document > col = db.getCollection("custorders");
        // Query to find top 5 customers based on total order amount
		
        AggregateIterable<Document> res = col.aggregate(Arrays.asList(
                Aggregates.unwind("$orders"),
                Aggregates.group("$custkey",
                        Accumulators.sum("totalOrderAmount", "$orders.totalprice")),
                Aggregates.lookup("customer", "_id", "custkey", "customer"),
                Aggregates.project(
                        Projections.fields(
                                Projections.excludeId(),
                                Projections.computed("customerName", new Document("$arrayElemAt", Arrays.asList("$customer.name", 0))),
                                Projections.computed("totalOrderAmount", "$totalOrderAmount")
                        )
                ),
                Aggregates.sort(Sorts.descending("totalOrderAmount")),
                Aggregates.limit(5)
        ));

        return res.iterator();
    }
    /**
	//* Returns the Mongo database being used.
	//* 
	//* @return
	//*         
	//Mongo database
	//*/
    public MongoDatabase getDb() 
	{
        return db;
    }
    ///**
	//* Outputs a cursor of MongoDB results in string form.
	//* 
	//* @param cursor
	//*               
	//Mongo cursor
	//* @return
	//*         
	//results as a string
	//*/
    public static String toString(MongoCursor < Document > cursor) 
	{
        StringBuilder buf = new StringBuilder();
        int count = 0;
        buf.append("Rows:\n");
        if (cursor != null) {
            while (cursor.hasNext()) {
                Document obj = cursor.next();
                buf.append(obj.toJson());
                buf.append("\n");
                count++;
            }
            cursor.close();
        }
        buf.append("Number of rows: " + count);
        return buf.toString();
    }
}