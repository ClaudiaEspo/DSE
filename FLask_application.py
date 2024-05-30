from flask import Flask, request, render_template, jsonify
import redis

app = Flask(__name__)

redis_host = 'localhost'
redis_port = 6386
r = redis.Redis(host=redis_host, port=redis_port)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/search', methods=['POST'])
def search():
    field = request.form.get('field')
    value = request.form.get('value')

    if not field or not value:
        return render_template('index.html', error='Field and value parameters are required')

    # Search in Redis
    result = []
    keys = r.keys('film:*')

    for key in keys:
        if r.hget(key, field) == value.encode():
            movie_data = r.hgetall(key)
            movie_data = {k.decode(): v.decode() for k, v in movie_data.items()}
            result.append(movie_data)

    if result:
        return render_template('index.html', results=result)
    else:
        return render_template('index.html', message='No results found')


if __name__ == '__main__':
    app.run(debug=True)
