import redis
import pandas as pd
import time
import threading
redis_host = 'localhost'
redis_port = 6386
r = redis.Redis(host=redis_host, port=redis_port)

def filtering_rating(redis_connection, min_rating):
    start_time = time.time()
    high_rating_films = []
    for film_key in redis_connection.keys('*'):
        rating_bytes = redis_connection.hget(film_key, 'score')
        if rating_bytes is not None:
            rating = float(rating_bytes.decode())
            if rating > min_rating:
                high_rating_films.append(film_key.decode())
        else:
            print(f"Warning: No rating found for film {film_key.decode()}")
    end_time = time.time()
    execution_time = end_time - start_time
    print("Films with IMDb rating>=",min_rating," are ", len(high_rating_films))
    print("Execution Time for the function is :", execution_time, "seconds")
    return high_rating_films

print_lock = threading.Lock()

def threads_1(client_id, min_rating):
    start_time = time.time()
    with print_lock:
        print(f"Client {client_id}: ")
        filtering_rating(r, min_rating)
    end_time = time.time()
    execution_time = end_time - start_time
    with print_lock:
        print(f"Client {client_id} completed in {execution_time:.2f} seconds")
        
def threads_2(client_id, min_rating):
    start_time = time.time()
    with print_lock:
        print(f"Client {client_id}: ")
        filtering2(r, min_rating)
    end_time = time.time()
    execution_time = end_time - start_time
    with print_lock:
        print(f"Client {client_id} completed in {execution_time:.2f} seconds")

def find_movie_by_writer(redis_connection, writer_name):
    print("Reserch of the movies with writer : ", writer_name)
    start_time = time.time()
    movie_keys = []
    writer_name_lower = writer_name.lower()
    for film_key in redis_connection.keys("*"):
        stored_movie_name_bytes = redis_connection.hget(film_key, 'writer')
        if stored_movie_name_bytes is not None:
            stored_movie_name = stored_movie_name_bytes.decode().lower()
            if stored_movie_name == writer_name_lower:
                movie_data = {field.decode(): redis_connection.hget(film_key, field).decode() for field in
                              redis_connection.hkeys(film_key)}
                movie_keys.append(movie_data)
    end_time = time.time()
    execution_time = end_time - start_time
    if movie_keys:
        print(f"Movie details: {movie_keys}")
        print(f"Execution Time for the function is: {execution_time} seconds")
    else:
        return f"No movies found with writer '{writer_name}'", execution_time

def find_movie_by_writer_director(redis_connection, director, writer):
    print("Reserch of the movies with writer : ", writer, " and director :", director)
    start_time = time.time()
    movie_keys = []
    split_director = director.lower().split()
    split_writer = writer.lower().split()
    for film_key in redis_connection.keys("*"):
        stored_director_bytes = redis_connection.hget(film_key, "director")
        if stored_director_bytes is not None:
            stored_director = stored_director_bytes.decode().lower()
            split_stored_director = stored_director.split()
            all_director_words_found = all(word in split_stored_director for word in split_director)
        else:
            all_director_words_found = False
        stored_writer_bytes = redis_connection.hget(film_key, "writer")
        if stored_writer_bytes is not None:
            stored_writer = stored_writer_bytes.decode().lower()
            split_stored_writer = stored_writer.split()
            all_writer_words_found = all(word in split_stored_writer for word in split_writer)
        else:
            all_writer_words_found = False
        if all_director_words_found and all_writer_words_found:
            movie_data = {field.decode(): redis_connection.hget(film_key, field).decode() for field in redis_connection.hkeys(film_key)}
            end_time = time.time()  
            execution_time = end_time - start_time
            print(f"Movie details: {movie_data}")
            print(f"Execution Time for the function is: {execution_time} seconds")
            break
    end_time = time.time()
    execution_time = end_time - start_time
    return f"Movie not found with director '{director}' and writer '{writer}'", execution_time

def aggregate_filtering(high_rating_films):
    start_time = time.time()
    num_high_rating_films = len(high_rating_films)
    if num_high_rating_films == 0:
        average_rating = 0
    else:
        total_rating = sum(float(r.hget(film_key, 'score').decode()) for film_key in high_rating_films)
        average_rating = total_rating / num_high_rating_films
    end_time = time.time()
    execution_time = end_time - start_time
    print("Average rating  of the film:", average_rating)
    print("Execution Time for the calculate the average  is :", execution_time, "seconds")

def calculate_metrics(redis_connection):
    print("Metrics calculator about the server")
    info_data = redis_connection.info()
    used_memory = info_data['used_memory']
    uptime_in_seconds = info_data['uptime_in_seconds']
    total_commands_processed = info_data['total_commands_processed']
    connected_clients = info_data['connected_clients']
    print(f"Used Memory: {used_memory} bytes")
    print(f"Uptime: {uptime_in_seconds} seconds")
    print(f"Total Commands Processed: {total_commands_processed}")
    print(f"Connected Clients: {connected_clients}")
    used_memory_bytes = info_data['used_memory']
    total_system_memory_bytes = info_data['total_system_memory']
    memory_usage_percent = (used_memory_bytes / total_system_memory_bytes) * 100
    print(f"Memory Usage Percentage: {memory_usage_percent:.2f}%")


def filtering2(redis_connection, min_rating=8.0):
    start_time = time.time()
    high_rating_films = []
    for film_key in redis_connection.keys('*'):
        rating_bytes = redis_connection.hget(film_key, 'Critic_Score')
        if rating_bytes is not None:
            rating = float(rating_bytes.decode())
            if rating > min_rating:
                high_rating_films.append(film_key.decode())

    end_time = time.time()
    execution_time = end_time - start_time
    print("Films with IMDb rating>=", min_rating, " are ", len(high_rating_films))
    print("Execution Time for the function is :", execution_time, "seconds")
    return high_rating_films

def aggregate_filtering2(high_rating_films):
    start_time = time.time() 
    num_high_rating_films = len(high_rating_films)
    if num_high_rating_films == 0:
        average_rating = 0
    else:
        total_rating = sum(float(r.hget(film_key, 'Critic_Score').decode()) for film_key in high_rating_films)
        average_rating = total_rating / num_high_rating_films
    end_time = time.time()
    execution_time = end_time - start_time
    print("total number of elements with ratings >= 8.0:", num_high_rating_films)
    print("Average rating  of fil with rate>= 8:", average_rating)
    print("Execution Time for the function is :", execution_time, "seconds")

def find_movie_by_publisher(redis_connection, publisher):
    start_time = time.time()  
    movie_keys = []
    writer_name_lower = publisher.lower() 
    for film_key in redis_connection.keys("*"):
        stored_movie_name_bytes = redis_connection.hget(film_key, 'Publisher')
        if stored_movie_name_bytes is not None:
            stored_movie_name = stored_movie_name_bytes.decode().lower()
            if stored_movie_name == writer_name_lower: 
                movie_data = {field.decode(): redis_connection.hget(film_key, field).decode() for field in
                              redis_connection.hkeys(film_key)}
                movie_keys.append(movie_data)  

    end_time = time.time() 
    execution_time = end_time - start_time

    if movie_keys:  
        print(f"Movie with Publisher:{publisher}, are {len(movie_keys)}")
        print(f"Time taken for search: {execution_time} seconds")
    else:
        return f"No movies found with writer '{publisher}'", execution_time


def find_movie_by_publisher_genre(redis_connection, publisher, Genre):
    start_time = time.time()
    movie_keys = []
    split_director = publisher.lower().split()
    split_writer = Genre.lower().split()

    for film_key in redis_connection.keys("*"):
        stored_director_bytes = redis_connection.hget(film_key, "Publisher")
        if stored_director_bytes is not None:
            stored_director = stored_director_bytes.decode().lower()
            split_stored_director = stored_director.split()
            all_director_words_found = all(word in split_stored_director for word in split_director)

        else:
            all_director_words_found = False

        stored_writer_bytes = redis_connection.hget(film_key, "Genre")
        if stored_writer_bytes is not None:
            stored_writer = stored_writer_bytes.decode().lower()
            split_stored_writer = stored_writer.split()
            all_writer_words_found = all(word in split_stored_writer for word in split_writer)

        else:
            all_writer_words_found = False

        if all_director_words_found and all_writer_words_found:
            movie_data = {field.decode(): redis_connection.hget(film_key, field).decode() for field in redis_connection.hkeys(film_key)}
            end_time = time.time()  
            execution_time = end_time - start_time
            print(f"Movie details: {movie_data}")
            print(f"Time taken for search: {execution_time} seconds")
            break
    end_time = time.time()
    execution_time = end_time - start_time

    return f"Movie not found with director '{publisher}' and writer '{Genre}'", execution_time

r.flushdb()
#first dataset of 1646 elements
df = pd.read_csv("/mnt/c/Users/espoc/Desktop/DSE/phase 3/movies - Copia.csv", encoding="latin1")
for index, row in df.iterrows():
    movie_id = row['name']
    movie_data = row.to_dict()
    for campo, valore in movie_data.items():
        r.hset(f'film:{movie_id}', campo, valore)

database_size = r.dbsize()
print(f"New database,Number of elements in the database:", database_size)
result= filtering_rating(r, 8.0)
aggregate_filtering(result)
result1=filtering_rating(r, 4.0)
aggregate_filtering(result1)
result2=filtering_rating(r, 6.0)
aggregate_filtering(result2)
writer1 = "Stephen King"
find_movie_by_writer(r, writer1)
writer2 = "Dan Aykroyd"
find_movie_by_writer(r, writer2)
director1 = "Stanley Kubrick"
find_movie_by_writer_director(r,director1 ,writer1)

calculate_metrics(r)

num_clients = 10
print("simulation wiht multiple clients")
min_ratings = [8.0, 4.0, 6.0]
threads = []
for i in range(num_clients):
    client_id = i + 1
    min_rating = min_ratings[i % len(min_ratings)] 
    thread = threading.Thread(target=threads_1, args=(client_id, min_rating))
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

r.flushdb()
#second dataset of 7512  elements

df = pd.read_csv("/mnt/c/Users/espoc/Desktop/DSE/phase 3/movies.csv", encoding="latin1")

for index, row in df.iterrows():
    movie_id = row['name']
    movie_data = row.to_dict()
    for campo, valore in movie_data.items():
        r.hset(f'film:{movie_id}', campo, valore)

database_size = r.dbsize()
print(f"New database, Number of elements in the database:", database_size)


result= filtering_rating(r, 8.0)
aggregate_filtering(result)
result1=filtering_rating(r, 4.0)
aggregate_filtering(result1)
result2=filtering_rating(r, 6.0)
aggregate_filtering(result2)
writer1 = "Stephen King"
find_movie_by_writer(r, writer1)
writer2 = "Dan Aykroyd"
find_movie_by_writer(r, writer2)

director1 = "Stanley Kubrick"
find_movie_by_writer_director(r,director1 ,writer1)

calculate_metrics(r)
num_clients = 10
min_ratings = [8.0, 4.0, 6.0]

threads = []
for i in range(num_clients):
    client_id = i + 1
    min_rating = min_ratings[i % len(min_ratings)] 
    thread = threading.Thread(target=threads_1, args=(client_id, min_rating))
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

r.flushdb()

#third dataset of 11563  elements
df = pd.read_csv("/mnt/c/Users/espoc/Desktop/DSE/phase 3/Video_Games_Sales_as_at_22_Dec_2016.csv", encoding="latin1")

for index, row in df.iterrows():
    movie_id = row['Name']
    movie_data = row.to_dict()
    for campo, valore in movie_data.items():
        r.hset(f'film:{movie_id}', campo, valore)

database_size = r.dbsize()
print(f"New Database, Number of elements in the database:", database_size)
result= filtering2(r, 80.0)
aggregate_filtering2(result)
result1=filtering2(r, 40.0)
aggregate_filtering2(result1)
result2=filtering2(r, 60.0)
aggregate_filtering2(result2)


find_movie_by_publisher(r, "Nintendo")
find_movie_by_publisher_genre(r, "Nintendo", "Racing")

calculate_metrics(r)

num_clients = 10
min_ratings = [80.0, 40.0, 60.0]

threads = []
for i in range(num_clients):
    client_id = i + 1
    min_rating = min_ratings[i % len(min_ratings)] 

    thread = threading.Thread(target=threads_2, args=(client_id, min_rating))
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

r.flushdb()






r.close()
