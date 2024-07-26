from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from astrapy import DataAPIClient
import csv
import json

class CassandraDB:
    def __init__(self):
        self.session = None

    def connect(self):
        try:
            # Load credentials from the JSON file
            with open(r'C:\Users\dives\OneDrive\Desktop\BigData Assignment\Cassandra\lib\bdm-cassandradb-1-token.json', 'r') as f:
                credentials = json.load(f)
            
            cloud_config = {
                'secure_connect_bundle': r'C:\Users\dives\OneDrive\Desktop\BigData Assignment\Cassandra\lib\secure-connect-bdm-cassandradb-1.zip'
            }
            auth_provider = PlainTextAuthProvider(credentials['CLIENT_ID'], credentials['CLIENT_SECRET'])
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            self.session = cluster.connect('bdmcassandradb')
            print("\n\nConnected to Cassandra")
        except Exception as e:
            print(f"Error connecting to Cassandra: {e}")
        
    def create_table(self):
        # TODO: create a table with proper schema, PLEASE NAME IT "Customer"
        drop_query="""DROP TABLE Customer"""
        self.session.execute(drop_query)
        
        create_query = """
        CREATE TABLE IF NOT EXISTS Customer (
            id int PRIMARY KEY,
            gender TEXT,
            age INT,
            number_of_kids INT
        )
        """
        self.session.execute(create_query)
        print("\n\nTable Customer Created Successfully")

    def load_data(self):
        #TODO: load customer.csv into the table created
        print("\n\nLoading data into Customer from Customer.csv.....")
        query = """
        BEGIN BATCH
        """
        with open('data\customers.csv', 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                query += """
                INSERT INTO Customer (id, gender, age, number_of_kids)
                VALUES ({id}, '{gender}', {age}, {number_of_kids});
                """.format(
                    id=int(row['id']),
                    gender=row['gender'],
                    age=int(row['age']),
                    number_of_kids=int(row['number_of_kids'])
                )
                
        query += """
        APPLY BATCH;
        """
        self.session.execute(query)
        print("\n\nData Loaded into Customer from Customer.csv")
        
        
    def query_1(self):
        # TODO: write query 1 that returns the age of the customer whose id is 979863
        print("\n\nExecuting Query 1...")
        print("Query to get the age of a customer whose id is 979863")
        query = "SELECT age FROM Customer WHERE id=979863"
        result = self.session.execute(query)
        for row in result:
            return row.age


    def query_2(self):
        #TODO: write query 2 that returns information of customers who are “MALE” and age is 25 or 35
        print("\n\nExecuting Query 2...")
        print("Query to get the Customer Information for all male whose age is 25 or 35")
        query = "SELECT * FROM Customer WHERE gender='MALE' AND age IN (25, 35) ALLOW FILTERING"
        result = self.session.execute(query)
        return list(result)


if __name__ == '__main__':
    client = CassandraDB()
    client.connect()
    client.create_table()
    client.load_data()
    age = client.query_1()
    print(f"The age of the customer with ID 979863 is: {age}")
    males_25_35 = client.query_2()
    print("Information of male customers aged 25 or 35:")
    for customer in males_25_35:
        print(customer)
