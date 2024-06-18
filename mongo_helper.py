import random
import string
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.collation import Collation
from dotenv import load_dotenv
from datetime import datetime
import os
import json
import re

def load_user_password():
    """
    Load the username and password from the .env file
    """
    if not os.path.isfile('.env'):
        print("\n\nError: No .env file found in the repository.\n\n")
        return None, None

    load_dotenv()
    return os.getenv('username'), os.getenv('password'), os.getenv('server_address')

def connect_to_mongo(username = None, password = None, server_address = None, debug = False):
    """
    DESCRIPTION:
        Return the MongoClient object
        Will load username, password, and server_address from the .env file
            if not provided as arguments.

    INPUT SIGNATURE:
        username: MongoDB username (string)
        password: MongoDB password (string)
        server_address: MongoDB server address (string)

    OUTPUT SIGNATURE:
        client: MongoClient object

    CAUTION:
        The user of manually setting username, password, and server_address
        when calling this function is highly discouraged. They are intended
        only to be used for quick connection to different MongoDB deployments
        when testing the application. The recommended way is to modify them
        from the .env file that should be in the same directory as this file.
    """

    if (username is None) or (password is None) or (server_address is None):
        username, password, server_address = load_user_password()
        uri = "mongodb+srv://" + username + ":" + password + server_address

    uri = "mongodb+srv://" + username + ":" + password + server_address
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        if debug:
            print("\n\n\n\n_____________________________\n")
            print("Pinged your deployment. You successfully connected to MongoDB!")
            print("_____________________________")
            print("_____________________________\n")
            print("Access the application at\nhttp://localhost:3421")
            print("_____________________________\n\n\n\n")
    except Exception as e:
        print("\n\n\n\n_____________________________\n")
        print("UNABLE TO CONNECT TO DATABASE")
        print("_____________________________\n\n\n\n")
        print(e)

    return client

def get_collection_legacy(client, database_name, collection_name):
    """
    Return the collection from the database
    """
    db = client[database_name]
    collection = db[collection_name]
    return collection

def upload_to_mongoDB_legacy(collection, document):
    """
    DESCRIPTION:
        Upload a document to the MongoDB collection.
    
    INPUT SIGNATURE:
        collection: MongoDB collection object
        document: a dictionary
    """

    try:
        # Upload the document to the MongoDB collection
        result = collection.insert_one(document)

        # Print the inserted document's ID
        print(f"Document uploaded successfully. ID: {result.inserted_id}")

    except Exception as e:
        print(f"Error uploading document: {e}")

def upload_to_mongoDB(database_name, collection_name, document, check_for_SKU_duplicates = True):
    """
    DESCRIPTION:
        Upload a document to the MongoDB collection.
    
    INPUT SIGNATURE:
        database_name: MongoDB database name (string)
        collection_name: MongoDB collection name (string)
        document: the document to be uploaded (dictionary)
        check_for_SKU_duplicates: if True, check if the SKU already exists in the collection (boolean)

    OUTPUT SIGNATURE:
        The function will prints out debug messages if the upload is successful or not.
            This message is generated through the upload_to_mongoDB_helper function.
    """

    client = connect_to_mongo()
    db = client[database_name]
    collection = db[collection_name]

    # loop through every field of the document
    # if there is a field that is an empty string, replace it with None
    for key, value in document.items():
        if value == "":
            document[key] = None

    if collection_name in db.list_collection_names(): # if collection exists

        if check_for_SKU_duplicates:

            if SKU_is_in_use(database_name, collection_name, document['SKU']):
                print("\n\n\nSKU already exists. Upload HALTED.\n\
                    Check if this is a DUPLICATE entry.\n\n\n")

            if 'SKU' not in document or not document['SKU']:
                document['SKU'] = generate_unique_sku(database_name, collection_name)
        
        upload_to_mongoDB_helper(client, collection, document)
        
    else:
        upload_to_mongoDB_helper(client, collection, document)
        update_missing_sku(database_name, collection_name) # if the uploaded object has not SKU, generate one
        create_index_for_field(database_name, collection_name, ['Concatenation'])

def upload_to_mongoDB_helper(client_object, collection_object, document):
    """
    CAUTION:
        DO NOT USE THIS FUNCTION DIRECTLY. USE upload_to_mongoDB INSTEAD.
        
    DESCRIPTION:
        Upload a document to the MongoDB collection.
    """

    try:
        # Upload the document to the MongoDB collection
        result = collection_object.insert_one(document)

        # Print the inserted document's ID
        print(f"Document uploaded successfully. ID: {result.inserted_id}")
        client_object.close()
        return True

    except Exception as e:
        print(f"Error uploading document: {e}")
        client_object.close()
        return False
    
def update_missing_sku(database_name, collection_name):
    """
    CAUTION:
        DO NOT USE THIS FUNCTION DIRECTLY.
        This function is only meant to be a helper for upload_to_mongoDB().

    DESCRIPTION:
        Check the only entry in the MongoDB database for the SKU field. If missing or empty, generate an SKU and update the entry.

    INPUT SIGNATURE:
        database_name: MongoDB database name (string)
        collection_name: MongoDB collection name (string)

    OUTPUT SIGNATURE:
        True if the SKU is updated successfully or already exists, False otherwise
    """
    client = connect_to_mongo()
    db = client[database_name]
    collection = db[collection_name]

    # Retrieve the only entry in the collection # function doesn't work if there are
    # multiple entries in the collection
    entry = collection.find_one()

    if entry is not None:  # Check if entry exists
        if 'SKU' not in entry or not entry['SKU']:
            new_sku = generate_unique_sku(database_name, collection_name)
            collection.update_one({}, {"$set": {"SKU": new_sku}})
            return True
        else:
            return True  # SKU already exists and not empty

    else:
        return False  # No entry found in the collection

def log_search_query(database_name, query):
    """
    DESCRIPTION:
        Log the search query to the MongoDB collection.

    INPUT SIGNATURE:
        keywords: a string of keywords that the user searched for
    """
    
    # Log the search query
    document = {'Database' : database_name,
                'Query' : query,
                'Time' : datetime.now()}

    upload_to_mongoDB('search_history', 'search_queries', document, check_for_SKU_duplicates = False)

def clean_search_query(keywords):
    """
    DESCRIPTION:
        Remove keywords that might lead to an empty result.
            1. RAM (and other variants)
            2. GB (and other variants)
            3. SSD (and other variants)
            4. HDD (and other variants)
    """

    # Case-insensitive removal
    keywords = re.sub(r'ram', '', keywords, flags=re.IGNORECASE)
    keywords = re.sub(r'gb', '', keywords, flags=re.IGNORECASE)
    keywords = re.sub(r'ssd', '', keywords, flags=re.IGNORECASE)
    keywords = re.sub(r'hdd', '', keywords, flags=re.IGNORECASE)

    # strip whitespaces
    keywords = keywords.strip()

    return keywords

def indexed_search_concatenation_field(database_name, collection_name, keywords):
    """
    DESCRIPTION:
        ONLY FOR A DATABASE THAT HAD BEEN INDEXED IN THE CONCATENATION FIELD THAT
        THE USER MIGHT PERFORM A SEARCH ON.

        The function is case-insensitive.

    INPUT SIGNATURE:
        database_name: MongoDB database name
        collection_name: MongoDB collection name
        keywords: a string of keywords that the user wants to search for

    OUTPUT SIGNATURE:
        result: a list of documents that matched the search terms
    """

    client = connect_to_mongo()
    db = client[database_name]
    collection = db[collection_name]

    keywords = clean_search_query(keywords)
    print("Searching for:", keywords)

    # Split the keywords into individual terms
    search_terms = keywords.split()

    # Create a list of regex patterns for each term
    regex_patterns = [{'Concatenation': {'$regex': f'.*{term}.*', '$options': 'i'}} for term in search_terms]

    if search_terms:
        # Combine the individual patterns with an "$and" operator
        query = {'$and': regex_patterns}
    else:
        query = {}

    # Perform a match search on the indexed 'Concatenation' field
    result = list(collection.find(query))

    # convert result to a list of dictionaries
    result = [dict(item) for item in result]
    print("Found {} results.".format(len(result)))

    # for each dictionary in the list,
    # convert the ObjectId to a string
    # 'Concatenation' keys
    for item in result:
        item['_id'] = str(item['_id'])  # Convert ObjectId to string
        item.pop('Concatenation')

    client.close()

    return result

def relevancy_indexed_search_concatenation_field(database_name, collection_name, keywords):
    """
    DESCRIPTION:
        ONLY FOR A DATABASE THAT HAD BEEN INDEXED IN THE CONCATENATION FIELD THAT
        THE USER MIGHT PERFORM A SEARCH ON.

        The function is case-insensitive.

        The main difference between this function and indexed_search_concatenation_field
        is that this function will return the results in descending order of relevancy and
        it will return all documents that have at least one keyword match.

    INPUT SIGNATURE:
        database_name: MongoDB database name
        collection_name: MongoDB collection name
        keywords: a string of keywords that the user wants to search for

    OUTPUT SIGNATURE:
        result: a list of documents that matched the search terms
    """

    client = connect_to_mongo()
    db = client[database_name]
    collection = db[collection_name]

    keywords = clean_search_query(keywords)
    print("Searching for:", keywords)

    # Split the keywords into individual terms
    search_terms = keywords.split()

    # Create a list of regex patterns for each term
    regex_patterns = [{'Concatenation': {'$regex': f'.*{term}.*', '$options': 'i'}} for term in search_terms]

    if search_terms:
        # Combine the individual patterns with an "$and" operator
        query = {'$or': regex_patterns} # or to return all documents with even just one match; and to return documents with all matches
    else:
        query = {}

    # Perform a match search on the indexed 'Concatenation' field
    cursor = collection.find(query)

    # Initialize a dictionary to store SKU and their scores
    scores = {}

    # Iterate through the cursor to count matches and calculate scores
    for document in cursor:
        score = 0
        for term in search_terms:
            if term.lower() in document['Concatenation'].lower():
                score += 1
        scores[document['SKU']] = score

    # Sort the scores dictionary by values (scores) in descending order
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    # Retrieve documents from the collection based on sorted SKU
    sorted_documents = [collection.find_one({'SKU': sku}) for sku, _ in sorted_scores]

    # Convert the documents to a list of dictionaries
    result = [dict(item) for item in sorted_documents if item]

    print("Found {} results.".format(len(result)))

    # Remove 'Concatenation' field
    for item in result:
        item.pop('Concatenation')
        # Convert ObjectId to string
        item['_id'] = str(item['_id'])

    client.close()

    return result

def create_index_for_field(database_name, collection_name, string_fields):
    """
    DESCRIPTION:
        Create a case-insensitive index for each specified string field

    INPUT SIGNATURE:
        database_name: MongoDB database name (string)
        collection_name: MongoDB collection name (string)
        string_fields: a list of fields containing strings to be indexed (list)
    """

    client = connect_to_mongo()
    db = client[database_name]
    collection = db[collection_name]

    # Create a case-insensitive index for each specified string field
    for field in string_fields:
        index_key = [(field, 1)]  # 1 means ascending order
        collation = Collation(locale = 'en', strength = 1) # 1 means case-insensitive
        collection.create_index(index_key, collation = collation)

    client.close()

def SKU_is_in_use(database_name, collection_name, SKU):
    """
    Check if an SKU is already in use in the database.
    """

    client = connect_to_mongo()
    db = client[database_name]
    collection = db[collection_name]

    # Check if a document with the given SKU already exists in the collection
    result = collection.find_one({"SKU": SKU})

    client.close()
    return result is not None

def generate_unique_sku(database_name, collection_name):
    """
    Generate an SKU that is unique to the collection.
    """
    while True:
        # Generate a random string of 8 characters
        random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        # Check if the generated SKU is already in use
        if not SKU_is_in_use(database_name, collection_name, random_string):
            return random_string

def remove_duplicate_entries(database_name, collection_name, field_with_duplication):

    client = connect_to_mongo()
    db = client[database_name]
    collection = db[collection_name]

    # Find duplicates based on the specified field
    pipeline = [
        {"$group": {"_id": f"${field_with_duplication}", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}}
    ]
    duplicate_entries = list(collection.aggregate(pipeline))

    original_rows = len(duplicate_entries)
    total_rows = sum(entry['count'] for entry in duplicate_entries)

    # Remove duplicates, keeping only one copy
    for entry in duplicate_entries:
        duplicates = list(collection.find({field_with_duplication: entry['_id']}))
        # Keep the first entry and delete the rest
        for i in range(1, len(duplicates)):
            collection.delete_one({"_id": duplicates[i]['_id']})

    client.close()

    print(f"\n\nDetected {total_rows} rows that are duplications of {original_rows} original rows based on the column [{field_with_duplication}]")
    print(f"Deleted {total_rows - original_rows} rows and kept {original_rows} original rows.\n\n")

def replace_empty_strings_with_none(database_name, collection_name):
    """
    This function iterates through a MongoDB collection and replaces empty strings with None.
    """

    client = connect_to_mongo()
    db = client[database_name]
    collection = db[collection_name]
    cursor = collection.find()

    for document in cursor:
        update_doc = {}
        for key, value in document.items():
            if value == "":
                update_doc[key] = None
        
        if update_doc:
            collection.update_one({"_id": document["_id"]}, {"$set": update_doc})

    client.close()