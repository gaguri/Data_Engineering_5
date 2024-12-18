import json
from pymongo import MongoClient


def connect_to_database():
    client = MongoClient()
    database = client['lab5']
    collection = database.jobs
    return collection


def read_json():
    with open('./tasks/3/task_3_item.json', 'r', encoding='utf-8') as file:
        return json.load(file)


def query_1(collection):
    return collection.delete_many(
    {
        '$or':
        [
            {'salary' : {'$lt': 25000}},
            {'salary': {'$gt': 175000}}
        ]
    })


def query_2(collection):
    return collection.update_many({},
    {
        '$inc':
        {
            'age': 1
        }
    })


def query_3(collection):
    return collection.update_many(
    {
        'job': {'$in': ['Врач', 'Медсестра']}
    },
    {
        '$mul':
        {
            'salary': 1.05
        }
    })


def query_4(collection):
    return collection.update_many(
    {
        'city': {'$in': ['Бильбао', 'Лас-Росас']}
    },
    {
        '$mul':
        {
            'salary': 1.07
        }
    })


def query_5(collection):
    return collection.update_many(
    {
        'job': {'$in': ['Водитель', 'Продавец']},
        'city': {'$in': ['Камбадос']},
        '$or': 
        [
            {'age': {'$gte': 30}},
            {'age': {'$lte': 50}}
        ] 
    },
    {
        '$mul':
        {
            'salary': 1.1
        }
    })


def query_6(collection):
    return collection.delete_many(
    {
        '$or': 
        [
            {'city': {'$in': ['Москва']}},
            {'salary': {'$gte': 150000}},
        ] 
    })


collection = connect_to_database()

data = read_json()

#collection.insert_many(data)

print(query_1(collection))
print(query_2(collection))
print(query_3(collection))
print(query_4(collection))
print(query_5(collection))
print(query_6(collection))