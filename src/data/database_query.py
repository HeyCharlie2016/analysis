from pymongo import MongoClient

def mongo_connect():
	client = MongoClient("mongodb+srv://ben:heycpass@cluster0-wszdu.mongodb.net/admin")
	return client.heycharlie
