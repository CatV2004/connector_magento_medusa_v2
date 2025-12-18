# Magento to Medusa Migration & Sync Tool

![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)

A production-grade, modular data migration pipeline designed to synchronize e-commerce data from Magento 2 (Enterprise) to Medusa (Headless Commerce). This tool handles complex EAV model transformation, data validation, and idempotent synchronization for core entities like Products, Categories, Customers, and Orders.

> **Project Genesis:** This tool was developed as part of a structured 12-week capstone project, evolving from foundational platform setup to a fully-featured, demo-ready migration pipeline.

## 📋 Table of Contents

- [✨ Core Features](#-core-features)
- [🏗️ Architecture & Design Philosophy](#️-architecture--design-philosophy)
- [📁 Project Structure](#-project-structure)
- [⚙️ Installation & Quick Start](#️-installation--quick-start)
- [🚀 Usage: CLI & Migration Pipeline](#-usage-cli--migration-pipeline)
- [🔧 Configuration: Mapping & Transformation](#-configuration-mapping--transformation)
- [📊 Data Model & Supported Entities](#-data-model--supported-entities)
- [🔍 Validation, Logging & DLQ](#-validation-logging--dlq)
- [🧪 Testing](#-testing)
- [🐳 Docker Deployment (Capstone Week 12)](#-docker-deployment-capstone-week-12)
- [📈 Project Roadmap (12-Week Plan)](#-project-roadmap-12-week-plan)
- [🤝 Contributing](#-contributing)
- [📄 License](#-license)

## ✨ Core Features

- **Full-Entity Migration:** Seamlessly migrate **Products, Categories, Customers, Orders, and Addresses**.
- **Declarative Schema Mapping:** YAML-based configuration for flexible, maintainable field mapping between Magento's EAV model and Medusa's data structure.
- **Extensible Transformation Pipeline:** Built-in data normalizers for prices, descriptions (HTML cleanup), image URLs, and status codes.
- **Robust Validation Framework:** Pre-flight and post-flight validation with configurable rules. Invalid records are routed to a **Dead Letter Queue (DLQ)** for analysis and reprocessing.
- **Production Resilience:** Implements **retry mechanisms with exponential backoff**, rate limiting, pagination handling, and checkpointing for long-running jobs.
- **Cloud-Native Media Handling:** **Automatic upload of product images** from Magento URLs to Cloudinary (or any S3-compatible service), optimizing and linking them in Medusa.
- **Delta Synchronization:** Smart sync based on `updated_at` timestamps to process only changed data.
- **Comprehensive Observability:** Structured logging, performance metrics, and detailed audit trails for every operation.

## 🏗️ Architecture & Design Philosophy

The tool follows a **modular, pipeline-based architecture**, inspired by ETL (Extract, Transform, Load) principles. Each component is loosely coupled, promoting testability and ease of extension.

```mermaid
graph LR
    A[Magento Source] -->|REST API| B(Extract Layer)
    B --> C[Raw Data]
    C --> D{Transform & Validate Layer}
    D -->|Valid| E[Transformed Data]
    D -->|Invalid| F[DLQ]
    E --> G(Load Layer)
    G -->|Medusa API| H[Medusa Target]
    I[YAML Config] --> D
    J[Cloudinary] --> G
