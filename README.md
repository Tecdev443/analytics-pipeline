# Real-Time Clickstream Analytics Platform

Enterprise clickstream analytics with real-time event processing, dbt transformations, and analytics-ready data models for business intelligence.

## Architecture

- **Collection**: Event SDK capturing user interactions (page views, clicks, purchases)
- **Ingestion**: Kafka topics for each event type
- **Processing**: Spark Structured Streaming for sessionization and metrics
- **Transformation**: dbt for dimensional modeling and data marts
- **Analytics**: Snowflake/BigQuery for BI tools and dashboards
- **Real-Time**: Kafka Streams for live dashboards

## Tech Stack

- Kafka (event backbone)
- PySpark (streaming + batch)
- dbt (ELT transformations)
- Snowflake/BigQuery (data warehouse)
- Docker Compose

## Project Structure

```
clickstream-analytics/
â”œâ”€â”€ src/
â”‚   â”œâ”€â”€ collectors/
â”‚   â”‚   â”œâ”€â”€ event_producer.py           # Synthetic/real event producer
â”‚   â”‚   â””â”€â”€ event_schemas.py            # Event schema definitions
â”‚   â”œâ”€â”€ processors/
â”‚   â”‚   â”œâ”€â”€ sessionization_job.py       # Group events into sessions
â”‚   â”‚   â”œâ”€â”€ aggregation_job.py          # Real-time metrics
â”‚   â”‚   â””â”€â”€ feature_engineering.py      # Cohort analysis features
â”‚   â””â”€â”€ consumers/
â”‚       â””â”€â”€ analytics_consumer.py       # Write to warehouse
â”œâ”€â”€ dbt/
â”‚   â”œâ”€â”€ models/
â”‚   â”‚   â”œâ”€â”€ staging/
â”‚   â”‚   â”‚   â””â”€â”€ stg_events.sql
â”‚   â”‚   â”œâ”€â”€ marts/
â”‚   â”‚   â”‚   â”œâ”€â”€ fct_sessions.sql
â”‚   â”‚   â”‚   â”œâ”€â”€ fct_user_behavior.sql
â”‚   â”‚   â”‚   â””â”€â”€ dim_users.sql
â”‚   â”‚   â””â”€â”€ intermediate/
â”‚   â”‚       â””â”€â”€ int_event_aggregates.sql
â”‚   â”œâ”€â”€ macros/
â”‚   â”‚   â””â”€â”€ generate_alias_name.sql
â”‚   â”œâ”€â”€ tests/
â”‚   â”‚   â””â”€â”€ assert_fact_row_counts.sql
â”‚   â”œâ”€â”€ dbt_project.yml
â”‚   â””â”€â”€ profiles.yml
â”œâ”€â”€ tests/
â”‚   â””â”€â”€ test_processors.py
â”œâ”€â”€ docker-compose.yml
â”œâ”€â”€ requirements.txt
â””â”€â”€ README.md
```

## Quick Start

### 1. Start Stack
```bash
docker-compose up -d
```

### 2. Produce Events
```bash
python src/collectors/event_producer.py
```

### 3. Run Stream Processing
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  src/processors/sessionization_job.py
```

### 4. Transform with dbt
```bash
cd dbt
dbt run
dbt test
```

## Features

- **Event Tracking**: 50+ event types (pageview, click, purchase, signup, etc.)
- **Sessionization**: Automatic session identification and metrics
- **Cohort Analysis**: User segmentation and behavior analysis
- **Real-Time Metrics**: Live dashboard support
- **Data Lineage**: Full lineage with dbt

## License

MIT
