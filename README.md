# Breeze — Real-Time Market Data & Order Execution System

A Python-based system for real-time market data ingestion, structured contract/order management, and automated order execution via a brokerage's WebSocket and REST APIs.

## Overview

This project explores the core engineering challenges behind live market data systems: maintaining a resilient real-time data feed, structuring incoming data for fast lookup, and executing orders reliably and concurrently.

## Key Components

- **Real-time data ingestion**: WebSocket-based connection for continuous streaming of tick, OHLCV, and market depth data, with reconnection and error-handling logic to keep the feed resilient.
- **Contract registry**: Structured mapping of instrument tokens to option and futures contracts for fast, consistent lookup across the system.
- **Multi-threaded order execution**: Concurrent order placement using a thread pool executor to handle multiple orders efficiently without blocking the data feed.
- **Options chain analysis**: Logic to scan a live options chain and identify near-the-money call/put pairs based on price proximity.
- **REST API integration**: Handles authentication, session management, and order-related API calls.

## Tech Stack

Python, WebSocket APIs, REST APIs, multi-threading (ThreadPoolExecutor)
