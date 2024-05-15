import numpy as np
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import BucketedRandomProjectionLSH

def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("MongoDB-Spark Connector").getOrCreate()

    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['music_database']
    tracks_collection = db['track_data']
    recommendation_collection = db['track_recommendations']  # Ensure this collection is created

    # Fetch data using PyMongo
    data = tracks_collection.find()
    
    # Prepare data for Spark ML
    rows = []
    for document in data:
        mfccs_mean = np.fromstring(document['MFCCs Mean'].strip('[]'), sep=' ')
        mfccs_std = np.fromstring(document['MFCCs Std'].strip('[]'), sep=' ')
        features = np.concatenate((mfccs_mean, mfccs_std))
        rows.append((document['TrackID'], document['Genre'], Vectors.dense(features)))

    # Create DataFrame
    df = spark.createDataFrame(rows, ["track_id", "genre", "features"])
    
    # StringIndexer for genre labels
    indexer = StringIndexer(inputCol="genre", outputCol="label")
    indexed = indexer.fit(df).transform(df)

    # Feature vector
    feature_vector = VectorAssembler(inputCols=["features"], outputCol="features_vector")
    feature_df = feature_vector.transform(indexed)
    
    # Split the data
    train_data, test_data = feature_df.randomSplit([0.8, 0.2])

    # Train KNN model using BucketedRandomProjectionLSH
    brp = BucketedRandomProjectionLSH(inputCol="features_vector", outputCol="hashes", bucketLength=2.0, numHashTables=3)
    model = brp.fit(train_data)
    
    # Transform training data
    transformed_train = model.transform(train_data)

    # Find approximate nearest neighbors and store in MongoDB
    for row in transformed_train.collect():
        neighbors = model.approxNearestNeighbors(transformed_train, row.features_vector, 5)
        recommendations = []
        for neighbor in neighbors.collect():
            recommendations.append({
                "track_id": row.track_id,
                "similar_track_id": neighbor.track_id,
                "similarity_score": neighbor.distCol
            })
        recommendation_collection.insert_many(recommendations)

    # Stop SparkSession
    spark.stop()

if __name__ == "__main__":
    main()

