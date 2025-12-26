# AWS Lakehouse Clickstream Pipeline

## Overview
This project demonstrates an end-to-end big data pipeline on AWS using
a lakehouse architecture (Bronze / Silver / Gold).

The system ingests streaming clickstream events, stores them in an 
S3-based
data lake, applies transformations using Spark (AWS Glue), and exposes
analytics-ready datasets for querying.

## Architecture Goals
- Handle streaming data at scale
- Support reprocessing and backfills
- Enforce schema and data quality
- Be fully reproducible using Infrastructure as Code
- Optimize for cost and observability

## High-Level Architecture
Producer → Streaming Ingestion → S3 Bronze → S3 Silver → S3 Gold → 
Analytics

## Data Model (Clickstream Events)
Each event represents a user interaction with a product or webpage.

Fields include:
- event_id (UUID)
- event_ts (timestamp)
- user_id
- session_id
- event_type
- page
- device and geo metadata

## Data Lake Structure
- Bronze: raw JSON events partitioned by date/hour
- Silver: cleaned Parquet events partitioned by date
- Gold: aggregated metrics partitioned by date

## Non-Goals
- Real-time dashboards
- ML feature serving

