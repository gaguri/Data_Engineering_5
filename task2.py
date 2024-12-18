import pandas as pd
import json
from pymongo import MongoClient, ASCENDING, DESCENDING


def connect_to_database():
    client = MongoClient()
    database = client['lab5']
    collection = database.jobs
    return collection


def read_csv():
    data = pd.read_csv('./tasks/2/task_2_item.csv', encoding='utf-8')
    return data


def query_1(collection):
    query = [
        {
            '$group': 
            {
                '_id': 'result',
                'max_salary': {'$max': '$salary'},
                'min_salary': {'$min': '$salary'},
                'avg_salary': {'$avg': '$salary'}
            }
        }
    ]
    return(list(collection.aggregate(query)))


def query_2(collection):
    query = [
        {
            '$group':
            {
                '_id': '$job',
                'count': {'$sum': 1}
            }
        }
    ]
    return(list(collection.aggregate(query)))


def query_3(collection):
    query = [
        {
            '$group':
            {
                '_id': '$city',
                'max_salary': {'$max': '$salary'},
                'min_salary': {'$min': '$salary'},
                'avg_salary': {'$avg': '$salary'}
            }
        }
    ]
    return(list(collection.aggregate(query)))


def query_4(collection):
    query = [
        {
            '$group':
            {
                '_id': '$job',
                'max_salary': {'$max': '$salary'},
                'min_salary': {'$min': '$salary'},
                'avg_salary': {'$avg': '$salary'}
            }
        }
    ]
    return(list(collection.aggregate(query)))


def query_5(collection):
    query = [
        {
            '$group':
            {
                '_id': '$city',
                'max_age': {'$max': '$age'},
                'min_age': {'$min': '$age'},
                'avg_age': {'$avg': '$age'}
            }
        }
    ]
    return(list(collection.aggregate(query)))


def query_6(collection):
    query = [
        {
            '$group':
            {
                '_id': '$job',
                'max_age': {'$max': '$age'},
                'min_age': {'$min': '$age'},
                'avg_age': {'$avg': '$age'}
            }
        }
    ]
    return(list(collection.aggregate(query)))


def query_7(collection):
    query = [
        {
            '$group':
            {
                '_id': '$age',
                'max_salary': {'$max': '$salary'}
            }
        },
        {
            '$sort':
            {
                '_id': ASCENDING
            }
        },
        {
            '$limit': 1
        }
    ]
    return(list(collection.aggregate(query)))


def query_8(collection):
    query = [
        {
            '$group':
            {
                '_id': '$age',
                'min_salary': {'$min': '$salary'}
            }
        },
        {
            '$sort':
            {
                '_id': DESCENDING
            }
        },
        {
            '$limit': 1
        }
    ]
    return(list(collection.aggregate(query)))


def query_9(collection):
    query = [
        {
            '$match':
            {
                'salary': {'$gt': 50000}
            }
        },
        {
            '$group':
            {
                '_id': '$city',
                'max_age': {'$max': '$age'},
                'min_age': {'$min': '$age'},
                'avg_age': {'$avg': '$age'}
            }
        },
        {
            '$sort':
            {
                'avg_age': DESCENDING
            }
        }
    ]
    return(list(collection.aggregate(query)))


def query_10(collection):
    query = [
        {
            '$match':
            {
                'city': {'$in': ['Алма-Ата', 'Сантьяго-де-Компостела', 'Фигерас']},
                'job': {'$in': ['Повар', 'Психолог', 'Продавец']},
                '$or': [
                    {'age': {'$gt': 18, '$lt': 25}},
                    {'age': {'$gt': 50, '$lt': 65}}
                ]
            }
        },
        {
            '$group':
            {
                '_id': 'result',
                'max_salary': {'$max': '$salary'},
                'min_salary': {'$min': '$salary'},
                'avg_salary': {'$avg': '$salary'}
            }
        }
    ]
    return(list(collection.aggregate(query)))


def query_11(collection):
    query = [
        {
            '$match':
            {
                'job': 'Оператор call-центра',
                'year': {'$gte': 2015, '$lte': 2017},
            }
        },
        {
            '$group':
            {
                '_id': '$year',
                'max_age': {'$max': '$age'},
                'min_age': {'$min': '$age'},
                'avg_age': {'$avg': '$age'},
                'avg_salary': {'$avg': '$salary'}
                
            }
        },
        {
            '$sort':
            {
                '_id': ASCENDING
            }
        }
    ]
    return(list(collection.aggregate(query)))


def save_queries(filename, result):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(result, file, ensure_ascii=False, indent=1)


collection = connect_to_database()

data = read_csv()
data_dict = data.to_dict(orient='records')

#collection.insert_many(data_dict)

save_queries('./results/2/query_1.json', query_1(collection))
save_queries('./results/2/query_2.json', query_2(collection))
save_queries('./results/2/query_3.json', query_3(collection))
save_queries('./results/2/query_4.json', query_4(collection))
save_queries('./results/2/query_5.json', query_5(collection))
save_queries('./results/2/query_6.json', query_6(collection))
save_queries('./results/2/query_7.json', query_7(collection))
save_queries('./results/2/query_8.json', query_8(collection))
save_queries('./results/2/query_9.json', query_9(collection))
save_queries('./results/2/query_10.json', query_10(collection))
save_queries('./results/2/query_11.json', query_11(collection))