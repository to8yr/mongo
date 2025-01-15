import pprint
from pymongo import MongoClient

connection_string = f"mongodb+srv://toby:Lev3l123@cluster0.8oqqe.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(connection_string)

dbs = client.list_database_names()
test_db = client.sample_analytics
collections = test_db.list_collection_names()

def insert_document():
    collection = test_db.test_collection
    document = {"name": "Toby", "age": 25}
    inserted_id = collection.insert_one(document).inserted_id
    print(inserted_id)

production = client.production
person_collection = production.person_collection

def create_documents():
    first_names = ["Toby", "John", "Jane", "Alice", "Bob", "Charlie"]
    last_names = ["Ralph", "Johnson", "Doe", "Brown", "White", "Black"]
    ages = [25, 30, 35, 40, 45, 50]

    docs = []
    for first_name, last_name, age in zip(first_names, last_names, ages):
        doc = {"first_name": first_name, "last_name": last_name, "age": age}
        docs.append(doc)
        #person_collection.insert_one(doc)

    person_collection.insert_many(docs)

printer = pprint.PrettyPrinter()

def find_all_people():
    people = person_collection.find()
    for person in people:
        pprint.pprint(person)

def find_toby():
    toby = person_collection.find_one({"first_name": "Toby"})
    pprint.pprint(toby)

def count_all_people():
    count = person_collection.count_documents(filter={})
    print("number of people:", count)

def get_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})

    pprint.pprint(person)

def get_age_range(min_age, max_age):
    query = {"$and": [
        {"age": {"$gte": min_age}},
        {"age": {"$lte": max_age}}
        ]}
    
    people = person_collection.find(query).sort("age")
    for person in people:
        pprint.pprint(person)

def project_columns():
    columns = {"_id": 0, "first_name": 1, "last_name": 1}
    people = person_collection.find({}, columns)
    for person in people:
        pprint.pprint(person)    

