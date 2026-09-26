# Real-Time Sentiment Analysis — Big Data Pipeline

> Kafka · Spark Structured Streaming · MLlib · Parquet · Streamlit
> Real-time brand reputation monitoring from live tweet streams.

---

## Table of Contents

1. [Overview](#overview)
2. [Screenshots — Dashboard](#screenshots--dashboard)
3. [Pipeline Architecture](#pipeline-architecture)
4. [Tech Stack](#tech-stack)
5. [Datasets](#datasets)
6. [Model Results](#model-results)
7. [Notebooks & Figures](#notebooks--figures)
8. [Quick Start — View the Dashboard](#quick-start--view-the-dashboard)
9. [Project Structure](#project-structure)

---

## Overview

The project builds a full Big Data processing chain for sentiment analysis:

1. **Offline training** of a classification model (Logistic Regression + TF-IDF) on
   1.6 million labeled tweets (Sentiment140).
2. **Real-time streaming**: tweet ingestion via Kafka, model inference in Spark
   Structured Streaming, predictions written to a Parquet Data Lake.
3. **Brand monitoring**: a dedicated stream of tweets mentioning Apple is
   continuously analyzed and visualized in a Streamlit dashboard (KPIs, sentiment
   trend, "bad buzz" alerts).

---

## Screenshots — Dashboard

**Overview** — brand status and key indicators

![Dashboard overview](docs/screenshots/dashboard_overview.png)

**Sentiment trend and overall breakdown**

![Trend and breakdown charts](docs/screenshots/dashboard_charts.png)

**Model confidence distribution and breakdown by product**

![Confidence histogram and product distribution](docs/screenshots/dashboard_distribution.png)

**Most negative tweets**

![Most negative tweets table](docs/screenshots/dashboard_negative_tweets.png)

**Running application stack (Docker Desktop)**

![Running Docker stack](docs/screenshots/docker_stack_running.png)

---

## Pipeline Architecture
![Pipeline architecture](docs/figures/architecture_pipeline.png)


### "Neutral" business layer

The model is binary (positive / negative). A post-processing layer reclassifies a
prediction as **Neutral** when the model's confidence is low:

```
confidence = max(P(positive), P(negative))

confidence < threshold       → Neutral
confidence ≥ threshold, positive label → Positive
confidence ≥ threshold, negative label → Negative
```

### "Bad buzz" detection

```
negative_rate = negatives / (positives + negatives)   [neutrals excluded]

negative_rate > threshold over the recent window → alert
```

---

## Tech Stack

| Component | Role |
|---|---|
| **Apache Kafka** | Real-time ingestion and messaging |
| **Apache ZooKeeper** | Coordination and metadata management for the Kafka cluster |
| **Kafka UI** | Web interface for monitoring Kafka topics and messages |
| **Apache Spark (Structured Streaming + MLlib)** | Batch and streaming processing, training and inference |
| **Apache Parquet** | Columnar Data Lake (model, predictions, metrics) |
| **Streamlit + Plotly** | Interactive visualization dashboard |
| **Docker / Docker Compose** | Stack orchestration (ZooKeeper, Kafka, Kafka UI, Spark master/worker) |

---

## Datasets

| | Sentiment140 | Apple Tweets |
|---|---|---|
| Volume | ~1,600,000 tweets | ~9,000 tweets |
| Role | Model training + large-scale demo stream | Business use case: Apple brand monitoring |
| Source | Stanford — Sentiment140 | Kaggle — Apple Twitter Sentiment Dataset |

> Raw files are not version-controlled (see `.gitignore`): download them separately
> and place them in `data/raw/`.

---

## Model Results

Latest evaluation on the test set (`src/evaluate_model.py`, metrics saved in
`data/metrics/sentiment_metrics.parquet`):

| Metric | Value |
|---|---|
| Accuracy | 0.766 |
| Precision | 0.766 |
| Recall | 0.766 |
| F1-score | 0.766 |
| AUC-ROC | 0.830 |

Confusion matrix (test set):

| | Predicted negative | Predicted positive |
|---|---|---|
| **Actual negative** | 119,880 (TN) | 39,875 (FP) |
| **Actual positive** | 34,675 (FN) | 124,133 (TP) |

---

## Notebooks & Figures

| Notebook | Content |
|---|---|
| `01_exploration_sentiment140.ipynb` | Data exploration and quality, class distribution |
| `02_preprocessing_sentiment140_pyspark.ipynb` | NLP cleaning, tokenization, TF-IDF |
| `03_training_model_spark_mllib.ipynb` | Model training, evaluation metrics |

**Class distribution (Sentiment140)**

![Class distribution](docs/figures/class_distribution.png)

**Training metrics**

![Training metrics](docs/figures/training_metrics.png)

---

## Quick Start — View the Dashboard

Full pipeline to run the application and observe the dashboard under real
conditions. All commands are to be run from the project root, in PowerShell
(Windows + Docker Desktop).

### 1. Start the Docker stack (Kafka + Spark)

```powershell
docker-compose build                              # first time only
docker-compose up -d
docker-compose --profile tools up -d spark-submit
docker-compose ps                                 # verify everything is "running"
```

### 2. Create the Kafka topics (first time only)

```powershell
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic sentiment_stream --partitions 4 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic apple_tweets --partitions 1 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092   # should list both topics
```

### 3. Prepare the model (if `data/models/sentiment_model/` is empty)

```powershell
docker exec spark-submit python src/preprocessing.py
docker exec spark-submit python src/train_model.py --sample 0.1   # quick version for testing
```

### 4. Start the Spark Streaming consumers (one terminal per command)

```powershell
# Terminal A
docker exec spark-submit python src/spark_streaming_consumer_sentiment140.py

# Terminal B
docker exec spark-submit python src/spark_streaming_consumer_apple.py
```

### 5. Start the Kafka producers (local Python environment, venv activated)

```powershell
# Terminal C
.\venv\Scripts\activate
python src/kafka_producer_sentiment140.py --max-rows 50000

# Terminal D
.\venv\Scripts\activate
python src/kafka_producer_apple.py --loop
```

### 6. Launch the dashboard

```powershell
# Terminal E
.\venv\Scripts\activate
streamlit run dashboard/app.py
```

Then open **http://localhost:8501**. The dashboard refreshes automatically and
displays KPIs, sentiment trend, and alerts as soon as the Parquet files are fed by
the consumers.

> Detailed guide with troubleshooting: [`docs/GUIDE_WINDOWS_DOCKER.md`](docs/GUIDE_WINDOWS_DOCKER.md)

---

## Project Structure

```
sentiment_pipeline/
│
├── notebooks/
│   ├── 01_exploration_sentiment140.ipynb          # Data exploration and quality
│   ├── 02_preprocessing_sentiment140_pyspark.ipynb # NLP cleaning, TF-IDF (PySpark)
│   └── 03_training_model_spark_mllib.ipynb        # Model training and evaluation
│
├── src/
│   ├── utils.py                                   # Shared functions (cleaning, UDFs, logger…)
│   ├── preprocessing.py                           # Batch NLP pipeline
│   ├── train_model.py                             # Model training (TF-IDF + Logistic Regression)
│   ├── evaluate_model.py                          # Evaluation: metrics, confusion matrix
│   ├── kafka_producer_sentiment140.py             # Kafka producer — Sentiment140 stream
│   ├── kafka_producer_apple.py                    # Kafka producer — Apple stream
│   ├── spark_streaming_consumer_sentiment140.py   # Spark Streaming consumer — Sentiment140 stream
│   └── spark_streaming_consumer_apple.py          # Spark Streaming consumer — Apple stream
│
├── dashboard/
│   └── app.py                                     # Streamlit application (real-time visualization)
│
├── config/
│   └── config.py                                  # Centralized configuration (paths, thresholds, Spark, Kafka)
│
├── data/                                          # Data Lake — not version-controlled (see .gitignore)
│   ├── raw/                                       # Raw CSVs (Sentiment140, Apple Tweets)
│   ├── processed/                                 # Cleaned data (Parquet)
│   ├── models/sentiment_model/                    # Trained and serialized MLlib model
│   ├── streaming/
│   │   ├── sentiment140_predictions/              # Sentiment140 stream predictions
│   │   └── apple_predictions/                     # Apple stream predictions
│   └── metrics/                                   # Evaluation metrics (Parquet)
│
├── docs/
│   ├── GUIDE_WINDOWS_DOCKER.md                    # Detailed execution guide (Windows + Docker)
│   ├── figures/                                   # Charts generated by the notebooks
│   │   └── class_distribution.png
│   └── screenshots/                               # Dashboard screenshots (to be completed)
│
├── scripts/                                       # Utility scripts
├── docker-compose.yml                             # Kafka + Spark stack (Zookeeper, Kafka, Spark master/worker)
├── Dockerfile                                     # Custom Spark image (sentiment-spark)
├── requirements.txt                               # Python dependencies — local environment / Streamlit
├── requirements-spark.txt                         # Python dependencies — Spark containers
├── .gitignore
└── README.md
```


---

*Big Data Project — Real-time sentiment analysis pipeline.*

## Authors

This project was developed by Fatiha Khassil and Oumaima Lahkiar, students at ENSIAS.

- Supervised by: Mrs. Widad Elouataoui
