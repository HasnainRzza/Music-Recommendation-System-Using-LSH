import os
import librosa
from pymongo import MongoClient
import numpy as np
import pandas as pd
import csv
from tqdm import tqdm


def load_track_ids(metadata_path):
    metadata = pd.read_csv(metadata_path, index_col=0, header=[0, 1])
    metadata.columns = ['_'.join(col).strip() for col in metadata.columns.values]
    return metadata


def preprocess_tracks(path, ex):
    process = pd.read_csv(path, header=[0, 1], low_memory=False)
    track_ids = process[('x', 'track_id')]
    artist_name = process[('artist', 'name')]
    artist_location = process[('artist', 'location')]
    track_title = process[('track', 'title')]
    genre = process[('track', 'genre_top')]
    listens = process[('track', 'listens')]
    data = pd.DataFrame({
        'TrackID': track_ids,
        'Artist': artist_name,
        'Genre': genre,
        'Track_Title': track_title,
        'Listens': listens,
        'Location': artist_location
    })

    mer = pd.read_csv(ex)
    if isinstance(mer.columns, pd.MultiIndex):
        mer.columns = mer.columns.droplevel(0)

    data = pd.merge(data, mer, on='TrackID', how='inner')
    data = data.dropna()
    return data


def extract_features(file_path):
    try:
        y, sr = librosa.load(file_path, sr=None)
        mfccs = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=13)
        mfccs_mean = np.mean(mfccs, axis=1)
        mfccs_std = np.std(mfccs, axis=1)
        return mfccs_mean, mfccs_std
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None, None


def process_files_in_directory(directory, track_metadata):
    features = []
    file_names = os.listdir(directory)
    for file_name in tqdm(file_names, desc="Processing files", unit="files"):
        if file_name.endswith(".mp3"):
            track_id = file_name.replace('.mp3', '')
            file_path = os.path.join(directory, file_name)
            mfccs_mean, mfccs_std = extract_features(file_path)
            if mfccs_mean is not None and mfccs_std is not None:
                features.append((track_id, mfccs_mean, mfccs_std))
    return features


def store_features_to_csv(features, csv_file):
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['TrackID', 'MFCCs Mean', 'MFCCs Std'])
        for track_id, mfccs_mean, mfccs_std in features:
            writer.writerow([track_id, mfccs_mean, mfccs_std])


def load_data_to_mongodb(data, db_name='music_database', collection_name='track_data'):
    client = MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    collection = db[collection_name]
    data_dict = data.to_dict('records')
    collection.insert_many(data_dict)
    print("Data has been inserted into MongoDB.")


def main():
    metadata_path = "path/to/your/tracks.csv"
    directory = "path/yo/your/music/sample"
    output_path = "/path/to/your/ouputfile"
 
    track_metadata = load_track_ids(metadata_path)
    extracted_features = process_files_in_directory(directory, track_metadata)
    
    csv_file = os.path.join(output_path, "MFCCs.csv")
    store_features_to_csv(extracted_features, csv_file)
    
    final_data = preprocess_tracks(metadata_path, csv_file)
    final_csv_path = os.path.join(output_path, "MusicRecords.csv")
    final_data.to_csv(final_csv_path, index=False)
    
    load_data_to_mongodb(final_data)


if __name__ == "__main__":
    main()
