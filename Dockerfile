FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY mypy.py mypy.py
COPY movies.csv movies.csv
COPY film.csv film.csv
COPY Video.csv Video.csv 

CMD ["python", "mypy.py"]
