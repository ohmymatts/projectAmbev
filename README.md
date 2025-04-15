# Open Brewery DB Data Pipeline

A robust data pipeline that extracts brewery data from the Open Brewery DB API, processes it through a medallion architecture (bronze, silver, gold layers), with Airflow orchestration and Spark processing.

## Features

- **Medallion Architecture**: Implements bronze (raw), silver (cleaned), and gold (aggregated) layers
- **Orchestration**: Apache Airflow for workflow management
- **Processing**: PySpark for efficient data transformation
- **Containerized**: Docker-compose for easy setup
- **Monitoring**: Built-in metrics and logging
- **Testing**: Unit and integration tests

## Prerequisites

- Docker Engine (20.10.0+)
- Docker Compose (2.0.0+)
- Python 3.8+ (for local development)

## Quick Start

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/open-brewery-pipeline.git
   cd open-brewery-pipeline