from flask import Flask, request, render_template, redirect, url_for, flash, session,send_from_directory
from kafka import KafkaConsumer
from pymongo import MongoClient

import json
import os

app = Flask(__name__)
app.secret_key = 'your_secret_key'  
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['music_database']
user_collection = db['user_data']

# Dictionary to store similar track IDs for each track ID
similar_track_ids = {}

# Directory where MP3 files are stored
mp3_directory = '/home/hasnainraza/latest2/sample'

def consume_from_kafka():
    consumer = KafkaConsumer('track_recommendations_topic',
                             bootstrap_servers=['localhost:9092'],
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        track_data = message.value
        track_id = track_data.get('track_id')
        similar_id = track_data.get('similar_track_id')
        if track_id and similar_id:
            padded_track_id = str(track_id).zfill(6)  # Pad zeros to make it 6 digits
            padded_similar_id = str(similar_id).zfill(6)  # Pad zeros for similar track ID
            similar_track_ids.setdefault(padded_track_id, set()).add(padded_similar_id)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = user_collection.find_one({"username": username, "password": password})
        if user:
            session['username'] = username
            return redirect(url_for('songs'))
        else:
            flash('Invalid username or password')
    return render_template('login.html')

@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        name = request.form['name']
        username = request.form['username']
        password = request.form['password']
        if user_collection.find_one({"username": username}):
            flash('Username already exists')
        else:
            user_collection.insert_one({
                "name": name, 
                "username": username, 
                "password": password, 
                "prefer": []  # Initializing an empty list for preferences
            })
            session['username'] = username
            return redirect(url_for('songs'))
    return render_template('signup.html')


@app.route('/songs')
def songs():
    if 'username' not in session:
        return redirect(url_for('login'))
   
    return render_template('songs.html', padded_track_ids=similar_track_ids.keys())

@app.route('/logout')
def logout():
    session.pop('username', None)
    return redirect(url_for('index'))

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/play/<padded_track_id>')
def play_track(padded_track_id):

    if 'username' not in session:
        return redirect(url_for('login'))
    
    username = session['username']
    user = user_collection.find_one({"username": username})

    if padded_track_id not in user['prefer']:
        user_collection.update_one({"username": username}, {"$addToSet": {"prefer": padded_track_id}})
    


    mp3_file = f"{padded_track_id}.mp3"
    similar_ids = similar_track_ids.get(padded_track_id, set())
    similar_mp3_files = [f"{sid}.mp3" for sid in similar_ids]
    return render_template('play.html', mp3_file=mp3_file, similar_mp3_files=similar_mp3_files)

@app.route('/mp3/<path:filename>')
def serve_mp3(filename):
    return send_from_directory(mp3_directory, filename, mimetype='audio/mpeg')

if __name__ == '__main__':
    # Start Kafka consumer in a separate thread
    import threading
    kafka_thread = threading.Thread(target=consume_from_kafka)
    kafka_thread.daemon = True
    kafka_thread.start()
    
    # Start Flask app
    app.run(debug=True)



