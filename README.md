Breeze — Automated Market Data & Reporting System

A Python-based system built on ICICI Direct's Breeze API for ingesting real-time market data, processing it into structured, actionable signals, and automating reporting and risk controls.

Overview

This project focuses on the data engineering and operational side of financial systems: reliable real-time data ingestion, automated reconciliation-style processing, and risk-aware execution controls — the same core skills used in trade support, settlement, and post-trade operations roles.

Key Components


Real-time data ingestion: WebSocket-based connection to the Breeze API for continuous market data streaming, with reconnection and error-handling logic to keep the pipeline resilient.
Data processing pipeline: Structures incoming tick data into clean, queryable formats for downstream analysis and reporting.
Automated reporting: Generates position, P&L, and performance summaries on a scheduled basis, reducing manual reconciliation work.
Risk controls: Implements configurable position sizing and automated stop-loss/exit logic to enforce risk limits systematically rather than manually.
REST API integration: Handles authentication, session management, and order-related calls against the Breeze API.


Tech Stack

Python, pandas, NumPy, WebSocket APIs, REST APIs

Disclaimer

This is a personal, educational project built to explore real-time financial data systems and is not used for live/funded trading. It is intended to demonstrate data engineering, API integration, and systematic process design.

Status

Actively maintained as a learning project.
