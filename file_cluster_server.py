from rediscluster import RedisCluster
import pandas as pd
import time
import threading

print_lock = threading.Lock()
startup_nodes = [
    {"host": "redis1", "port": "6379"},
    {"host": "redis2", "port": "6379"},
    {"host": "redis3", "port": "6379"},
    {"host": "redis4", "port": "6379"},
    {"host": "redis5", "port": "6379"},
    {"host": "redis6", "port": "6379"}
]

rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)

def filtering_rating(redis_connection, min_rating):
    start_time = time.time()
    high_rating_films = []
    for film_key in redis_connection.keys('*'):
        rating_bytes = redis_connection.hget(film_key, 'score')
        if rating_bytes is not None:
            rating = float(rating_bytes)
            if rating > min_rating:
                high_rating_films.append(film_key)
        else:
            print(f"Warning: No rating found for film {film_key}")
    end_time = time.time()
    execution_time = end_time - start_time
    print("Films with IMDb rating>=",min_rating," are ", len(high_rating_films))
    print("Execution Time for the function is :", execution_time, "seconds")
    return high_rating_films

def find_movie_by_writer(redis_connection, writer_name):
    print("Reserch of the movies with writer : ", writer_name)
    start_time = time.time()  # Record start time
    movie_keys = []
    writer_name_lower = writer_name.lower()  
    for film_key in redis_connection.keys("*"):
        stored_movie_name_bytes = redis_connection.hget(film_key, 'writer')
        if stored_movie_name_bytes is not None:
            stored_movie_name = stored_movie_name_bytes.lower()
            if stored_movie_name == writer_name_lower:  
                movie_data = {field: redis_connection.hget(film_key, field) for field in
                              redis_connection.hkeys(film_key)}
                movie_keys.append(movie_data) 
    end_time = time.time() 
    execution_time = end_time - start_time
    if movie_keys: 
        #print(f"Movie details: {movie_keys}")
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
            stored_director = stored_director_bytes.lower()
            split_stored_director = stored_director.split()
            all_director_words_found = all(word in split_stored_director for word in split_director)
        else:
            all_director_words_found = False

        stored_writer_bytes = redis_connection.hget(film_key, "writer")
        if stored_writer_bytes is not None:
            stored_writer = stored_writer_bytes.lower()
            split_stored_writer = stored_writer.split()
            all_writer_words_found = all(word in split_stored_writer for word in split_writer)
        else:
            all_writer_words_found = False

        if all_director_words_found and all_writer_words_found:
            movie_data = {field: redis_connection.hget(film_key, field) for field in redis_connection.hkeys(film_key)}
            end_time = time.time()
            execution_time = end_time - start_time
            print(f"Movie details: {movie_data}")
            print(f"Execution Time for the function is: {execution_time} seconds")

            break
    end_time = time.time()  
    execution_time = end_time - start_time

    return f"Movie not found with director '{director}' and writer '{writer}'", execution_time

def aggregate_high_rating_films(high_rating_films):
    start_time = time.time()  
    num_high_rating_films = len(high_rating_films)
    if num_high_rating_films == 0:
        average_rating = 0
    else:
        total_rating = sum(float(rc.hget(film_key, 'score')) for film_key in high_rating_films)
        average_rating = total_rating / num_high_rating_films
    end_time = time.time()
    execution_time = end_time - start_time
    print("Average rating  of the film:", average_rating)
    print("Execution Time for the calculate the average  is :", execution_time, "seconds")
  
def flushdb_cluster(redis_connection):
    try:
        keys = redis_connection.keys('*')
        for key in keys:
            redis_connection.delete(key)
        print("Cluster database flushed successfully")
    except Exception as e:
        print(f"Error flushing cluster database: {e}")

def filter_high_rating_films2(redis_connection, min_rating):
    start_time = time.time()

    high_rating_films = []
    for film_key in redis_connection.keys('*'):
        rating_bytes = redis_connection.hget(film_key, 'Critic_Score')
        if rating_bytes is not None:
            rating = float(rating_bytes)
            if rating > min_rating:
                high_rating_films.append(film_key)

    end_time = time.time()
    execution_time = end_time - start_time
    print("Films with IMDb rating>=", min_rating, " are ", len(high_rating_films))
    print("Execution Time for the function is :", execution_time, "seconds")
    return high_rating_films

def aggregate_high_rating_films2(high_rating_films):
    start_time = time.time()
    num_high_rating_films = len(high_rating_films)
    if num_high_rating_films == 0:
        average_rating = 0
    else:
        total_rating = sum(float(rc.hget(film_key, 'Critic_Score')) for film_key in high_rating_films)
        average_rating = total_rating / num_high_rating_films
    end_time = time.time()
    execution_time = end_time - start_time
    print("Average rating  of film:", average_rating)
    print("Execution Time for the function is :", execution_time, "seconds")

def find_movie_by_publisher(redis_connection, publisher):
    start_time = time.time()
    movie_keys = []
    writer_name_lower = publisher.lower()
    for film_key in redis_connection.keys("*"):
        stored_movie_name_bytes = redis_connection.hget(film_key, 'Publisher')
        if stored_movie_name_bytes is not None:
            stored_movie_name = stored_movie_name_bytes.lower()
            if stored_movie_name == writer_name_lower:
                movie_data = {field: redis_connection.hget(film_key, field) for field in
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
            stored_director = stored_director_bytes.lower()
            split_stored_director = stored_director.split()
            all_director_words_found = all(word in split_stored_director for word in split_director)
        else:
            all_director_words_found = False

        stored_writer_bytes = redis_connection.hget(film_key, "Genre")
        if stored_writer_bytes is not None:
            stored_writer = stored_writer_bytes.lower()
            split_stored_writer = stored_writer.split()
            all_writer_words_found = all(word in split_stored_writer for word in split_writer)
        else:
            all_writer_words_found = False

        if all_director_words_found and all_writer_words_found:
            movie_data = {field: redis_connection.hget(film_key, field) for field in redis_connection.hkeys(film_key)}
            end_time = time.time()  # Record end time after finding movie
            execution_time = end_time - start_time
            print(f"Movie details: {movie_data}")
            print(f"Time taken for search: {execution_time} seconds")
            #eturn movie_data, execution_time  
            break  
    end_time = time.time()  
    execution_time = end_time - start_time
    return f"Movie not found with director '{publisher}' and writer '{Genre}'", execution_time

def threads_1(client_id, min_rating):
    start_time = time.time()
    with print_lock:
        print(f"Client {client_id}: ")
        filtering_rating(rc, min_rating)
    end_time = time.time()
    execution_time = end_time - start_time
    with print_lock:
        print(f"Client {client_id} completed in {execution_time:.2f} seconds")

def threads_2(client_id, min_rating):
    start_time = time.time()
    with print_lock:
        print(f"Client {client_id}: ")
        filter_high_rating_films2(rc, min_rating)
    end_time = time.time()
    execution_time = end_time - start_time
    with print_lock:
        print(f"Client {client_id} completed in {execution_time:.2f} seconds")


flushdb_cluster(rc)
#first dataset of 1646 elements
df = pd.read_csv("/app/film.csv", encoding="latin1")
for index, row in df.iterrows():
    user_id = row['name']
    movie_data = row.to_dict()
    for campo, valore in movie_data.items():
        rc.hset(f'film:{user_id}', campo, valore)

database_size = rc.dbsize()
print(f"Number of elements in the database: {database_size}")

result= filtering_rating(rc, 8.0)
aggregate_high_rating_films(result)
result1=filtering_rating(rc, 4.0)
aggregate_high_rating_films(result1)
result2=filtering_rating(rc, 6.0)
aggregate_high_rating_films(result2)
movie_name_to_find = "Stephen King"
find_movie_by_writer(rc, movie_name_to_find)
movie_name_to_find = "Dan Aykroyd"
find_movie_by_writer(rc, movie_name_to_find)
direct = "Stanley Kubrick"
writ = "Stephen King"
find_movie_by_writer_director(rc,direct ,writ)


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







flushdb_cluster(rc)
#first dataset of 7512 elements
df = pd.read_csv("/app/movies.csv", encoding="latin1")
for index, row in df.iterrows():
    user_id = row['name']
    movie_data = row.to_dict()
    for campo, valore in movie_data.items():
        rc.hset(f'film:{user_id}', campo, valore)

database_size1 = rc.dbsize()
print(f"New database, Number of elements in the database: {database_size1}")

result= filtering_rating(rc, 8.0)
aggregate_high_rating_films(result)
result1=filtering_rating(rc, 4.0)
aggregate_high_rating_films(result1)
result2=filtering_rating(rc, 6.0)
aggregate_high_rating_films(result2)

movie_name_to_find = "Stephen King"
find_movie_by_writer(rc, movie_name_to_find)

movie_name_to_find = "Dan Aykroyd"
find_movie_by_writer(rc, movie_name_to_find)


direct = "Stanley Kubrick"
writ = "Stephen King"
find_movie_by_writer_director(rc,direct ,writ)

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



flushdb_cluster(rc)
#first dataset of 11563 elements

df = pd.read_csv("/app/Video.csv", encoding="latin1")
for index, row in df.iterrows():
    user_id = row['Name']
    movie_data = row.to_dict()
    for campo, valore in movie_data.items():
        rc.hset(f'film:{user_id}', campo, valore)

database_size2 = rc.dbsize()
print(f"New database, Number of elements in the database: {database_size2}")


result4= filter_high_rating_films2(rc, 80.0)
aggregate_high_rating_films2(result4)
result5=filter_high_rating_films2(rc, 40.0)
aggregate_high_rating_films2(result5)
result2=filter_high_rating_films2(rc, 60.0)
aggregate_high_rating_films2(result2)


find_movie_by_publisher(rc, "Nintendo")
find_movie_by_publisher_genre(rc, "Nintendo", "Racing")



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




flushdb_cluster(rc)
rc.close()

