from pymongo import MongoClient, DESCENDING, ASCENDING
import pickle
import json

def connect_to_database():
    client = MongoClient()
    database = client['lab5']
    collection = database.jobs
    return collection


def read_pickle():
    with open('./tasks/1/task_1_item.pkl', 'rb') as file:
        return pickle.load(file)
    

def query_1(collection):
    result = list(collection.find({}, {'_id': 0})
                            .sort('salary', DESCENDING)
                            .limit(10))
    return result


def query_2(collection):
    result = list(collection.find({
                                'age': {'$lt': 30}},
                                {'_id': 0})
                            .sort('salary', DESCENDING)
                            .limit(15))
    return result


def query_3(collection):
    result = list(collection.find({
                                'city': 'Хихон',
                                'job': {'$in': ['Строитель', 'Инженер', 'Бухгалтер']}},
                                {'_id': 0})
                            .sort('age', ASCENDING)
                            .limit(10))
    return result


def query_4(collection):
    result = (collection.count_documents({
                                'age': {'$gt': 20, '$lt': 30},
                                'year': {'$gte': 2019, '$lte': 2022},
                                '$or': [
                                    {'salary': {'$gt': 50000, '$lte': 75000}},
                                    {'salary': {'$gt': 125000, '$lt': 150000}}
                                ]}))
    return result


def save_queries(filename, result):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(result, file, ensure_ascii=False, indent=1)

collection = connect_to_database()

data = read_pickle()
#collection.insert_many(data)

save_queries('./results/1/query_1.json', query_1(collection))
save_queries('./results/1/query_2.json', query_2(collection))
save_queries('./results/1/query_3.json', query_3(collection))
save_queries('./results/1/query_4.json', query_4(collection))

