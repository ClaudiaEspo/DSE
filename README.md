# Redis Performance Analysis

Performance comparison between Redis single server and cluster configurations using Python and Flask.

## 📋 Overview

This project tests Redis capabilities in different scenarios:
- Single Redis server vs Redis cluster (3 masters + 3 slaves)
- Performance across multiple dataset sizes (1.6K, 7.5K, 11.5K elements)
- Concurrent client simulation using threading

## 🛠️ Technologies

- Python 3.x
- Redis (redis-py)
- Flask
- Pandas
- Docker (for cluster setup)

## 🚀 Quick Start

### Installation

```bash
# Install dependencies
pip install -r requirements.txt
```

### Single Server

```bash
# Start Redis
redis-server --port 6386

# Run tests
python file_single_server.py

# Run Flask application
python FLask_application.py
```

### Cluster Setup

```bash
# Build and run Docker containers
docker build -t redis-cluster .

# Run cluster tests
python file_cluster_server.py
```

## 📊 Key Results

### Single Server
- Linear scaling with dataset size
- Filtering ~0.19s (1.6K) → ~1.42s (11.5K)
- Better for small-medium datasets

### Cluster
- Higher overhead due to node communication
- Filtering ~0.23s (1.6K) → ~2.20s (11.5K)
- High availability and fault tolerance

### Concurrent Requests
- Both configurations show increased latency
- Single server faster for smaller workloads
- Cluster overhead increases with dataset size

## 🔍 Main Functions

- `filtering_rating()` - Filter by minimum rating
- `find_movie_by_writer()` - Search by writer
- `find_movie_by_writer_director()` - Search by writer and director
- `aggregate_filtering()` - Calculate average ratings
- `calculate_metrics()` - Server statistics

## 📁 Files

```
.
├── FLask_application.py        # Flask web interface
├── file_single_server.py       # Single server tests
├── file_cluster_server.py      # Cluster tests
├── index.html                  # Web UI template
├── movies.csv                  # Dataset
├── Dockerfile                  # Docker configuration
├── requirements.txt            # Python dependencies
└── Claudia_Esposito-Phase5-DA.pdf  # Full documentation
```

## 👤 Author

**Claudia Antonella Esposito**  
Computer Science Engineer, Federico II Naples
