def get_connection_string():
    from dotenv import load_dotenv
    import os

    load_dotenv()
    return os.getenv("MONGO_URI", "mongodb://localhost:27017/")

def get_database_name():
    from dotenv import load_dotenv
    import os

    load_dotenv()
    return os.getenv("DATABASE_NAME", "lab_de3")

def get_database():
    from pymongo import MongoClient

    connection_string = get_connection_string()
    database_name = get_database_name()
    client = MongoClient(connection_string)

    try:
        client.admin.command("ping")
        print("\nSuccessfully connected to MongoDB")
    except Exception as e:
        print(e)
        client = None

    if client is None:
        return None
    

    return client.get_database(database_name)
