# Magento to Medusa Data Sync Tool

A professional, enterprise-grade data synchronization tool for migrating data from Magento 2 to Medusa e-commerce platform.

## Features

- **Dynamic Field Mapping**: YAML-based configuration for flexible field mapping
- **Multiple Entity Support**: Products, Categories, Customers, Orders
- **Data Transformation**: Built-in transformers for data normalization
- **Validation Framework**: Comprehensive validation with DLQ (Dead Letter Queue)
- **Batch Processing**: Efficient batch processing with rate limiting
- **Error Handling**: Robust error handling with retry mechanisms
- **Cloudinary Integration**: Automatic image upload and optimization
- **CLI Interface**: Easy-to-use command line interface
- **Comprehensive Logging**: Detailed logging for debugging and monitoring

## Installation

### Prerequisites

- Python 3.8+
- Magento 2 REST API access
- Medusa Admin API access
- Cloudinary account (optional, for image hosting)

### Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd connectors
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Copy environment template:
   ```bash
   cp .env.template .env
   ```

4. Configure your environment variables in .env:
   ```bash
   # Magento Configuration
   MAGENTO_BASE_URL=https://your-magento-store.com/rest/V1
   MAGENTO_TOKEN=your_magento_integration_token

   # Medusa Configuration
   MEDUSA_BASE_URL=https://your-medusa-store.com
   MEDUSA_API_KEY=your_medusa_api_key

   # Cloudinary (optional)
   CLOUDINARY_CLOUD_NAME=your_cloud_name
   CLOUDINARY_API_KEY=your_api_key
   CLOUDINARY_API_SECRET=your_api_secret

   # Sync Settings
   SYNC_BATCH_SIZE=50
   LOG_LEVEL=INFO
   ```