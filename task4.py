import pandas as pd
import json
from pymongo import MongoClient, DESCENDING, ASCENDING


def connect_to_database():
    client = MongoClient()
    database = client['lab5']
    collection = database.top
    return collection


def read_csv():
    data = pd.read_csv('./tasks/4/top_p1.csv', encoding='utf-8')
    data = data.drop(columns=['Unnamed: 0'])
    return data


def read_json():
    data = pd.read_json('./tasks/4/top_p2.json', encoding='utf-8')
    data = data.drop(columns=['Unnamed: 0'])
    return data


def query_1(collection):
    result = list(collection.find({}, {'_id': 0})
                            .sort('track_name', ASCENDING))
    return result


def query_2(collection):
    result = list(collection.find({
                                'danceability': {'$lt': 0.6}},
                                {'_id': 0})
                            .sort('album_release_year', ASCENDING)
                            .limit(10))
    return result


def query_3(collection):
    result = list(collection.find({
                                'album_name': 'Vessel',
                                'key_name': 'C'},
                                {'_id': 0})
                            .sort('track_name', ASCENDING))
    return result


def query_4(collection):
    result = (collection.count_documents({
                                'acousticness': {'$gt': 0.1, '$lt': 0.3},
                                'energy': {'$gte': 0.6, '$lte': 0.8},}))
    return result


def query_5(collection):
    result = list(collection.find({
                                'time_signature': {'$gte': 3, '$lte': 3.5}},
                                {'_id': 0}
                                )
                            .sort('track_name', DESCENDING))
    return result


def query_6(collection):
    query = [
        {
            '$group': 
            {
                '_id': 'loudness',
                'max_loudness': {'$max': '$loudness'},
                'min_loudness': {'$min': '$loudness'},
                'avg_loudness': {'$avg': '$loudness'}
            }
        }
    ]
    return(list(collection.aggregate(query)))


def query_7(collection):
    query = [
        {
            '$group':
            {
                '_id': '$album_name',
                'count': {'$sum': 1}
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


def query_8(collection):
    query = [
        {
            '$group':
            {
                '_id': '$mode_name',
                'max_duration_ms': {'$max': '$duration_ms'},
                'min_duration_ms': {'$min': '$duration_ms'},
                'avg_duration_ms': {'$avg': '$duration_ms'}
            }
        }
    ]
    return(list(collection.aggregate(query)))


def query_9(collection):
    query = [
        {
            '$group':
            {
                '_id': '$album_release_year',
                'max_valence': {'$max': '$valence'},
                'min_valence': {'$min': '$valence'},
                'avg_valence': {'$avg': '$valence'}
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


def query_10(collection):
    query = [
        {
            '$group':
            {
                '_id': '$key_mode',
                'max_danceability': {'$max': '$danceability'},
                'min_danceability': {'$min': '$danceability'},
                'avg_danceability': {'$avg': '$danceability'}
            }
        },
        {
            '$limit': 10
        }
    ]
    return(list(collection.aggregate(query)))


def query_11(collection):
    return collection.delete_many(
    {
        'liveness' : {'$gte': 0.5}
    })


def query_12(collection):
    return collection.update_many({},
    {
        '$inc':
        {
            'disc_number': 1
        }
    })


def query_13(collection):
    return collection.update_many(
    {
        'mode_name': 'major'
    },
    {
        '$mul':
        {
            'tempo': 0.9
        }
    })


def query_14(collection):
    return collection.update_many(
    {
        'album_name': {'$in': ['Scaled and Icy', 'Trench']},
        '$or': 
        [
            {'acousticness': {'$gte': 0.1}},
            {'acousticness': {'$lte': 0.6}}
        ] 
    },
    {
        '$mul':
        {
            'speechiness': 1.05,
            'instrumentalness': 1.1
        }
    })


def query_15(collection):
    return collection.delete_many(
    {
        'album_name': 'Spotify Sessions'
    })


def save_queries(filename, result):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(result, file, ensure_ascii=False, indent=1)


collection = connect_to_database()

data_csv = read_csv()
data_csv_dict = data_csv.to_dict(orient='records')

data_json = read_json()
data_json_dict = data_json.to_dict(orient='records')

# collection.insert_many(data_csv_dict)
# collection.insert_many(data_json_dict)

save_queries('./results/4/query_1.json', query_1(collection))
save_queries('./results/4/query_2.json', query_2(collection))
save_queries('./results/4/query_3.json', query_3(collection))
save_queries('./results/4/query_4.json', query_4(collection))
save_queries('./results/4/query_5.json', query_5(collection))
save_queries('./results/4/query_6.json', query_6(collection))
save_queries('./results/4/query_7.json', query_7(collection))
save_queries('./results/4/query_8.json', query_8(collection))
save_queries('./results/4/query_9.json', query_9(collection))
save_queries('./results/4/query_10.json', query_10(collection))
print(query_11(collection))
print(query_12(collection))
print(query_13(collection))
print(query_14(collection))
print(query_15(collection))
