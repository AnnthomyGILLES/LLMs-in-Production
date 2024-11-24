import logging
from datetime import datetime, timedelta

import yfinance as yf
from airflow import DAG
from airflow.operators.python import PythonOperator

from common.kafka_utils.kafka_producer import KafkaProducerWrapper

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "stock_data"

# List of stock symbols to track
STOCK_SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN"]


def fetch_stock_data(symbol):
    """Fetch stock data for a given symbol using yfinance"""
    try:
        stock = yf.Ticker(symbol)
        # Get today's data
        data = stock.history(period="1d")

        if not data.empty:
            stock_data = {
                "symbol": symbol,
                "timestamp": datetime.now().isoformat(),
                "open": float(data["Open"].iloc[-1]),
                "high": float(data["High"].iloc[-1]),
                "low": float(data["Low"].iloc[-1]),
                "close": float(data["Close"].iloc[-1]),
                "volume": int(data["Volume"].iloc[-1]),
            }
            return stock_data
        return None
    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {str(e)}")
        return None


def process_and_send_to_kafka(**context):
    """Process stock data and send to Kafka"""
    with KafkaProducerWrapper(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC) as producer:
        try:
            for symbol in STOCK_SYMBOLS:
                stock_data = fetch_stock_data(symbol)

                if stock_data:
                    # Using the send_message method from your wrapper
                    success = producer.send_message(stock_data)
                    if success:
                        logger.info(f"Successfully sent data for {symbol} to Kafka")
                    else:
                        logger.warning(f"Failed to send data for {symbol} to Kafka")
                else:
                    logger.warning(f"No data available for {symbol}")

        except Exception as e:
            logger.error(f"Error in processing and sending data: {str(e)}")
            raise


# Create the DAG
dag = DAG(
    "stock_data_pipeline",
    default_args=default_args,
    description="A DAG to fetch stock data and send to Kafka",
    schedule_interval="*/15 * * * *",  # Runs every 15 minutes
    catchup=False,
)

# Define the task
fetch_and_send_task = PythonOperator(
    task_id="fetch_and_send_stock_data",
    python_callable=process_and_send_to_kafka,
    dag=dag,
)

# Set task dependencies (if you add more tasks later)
fetch_and_send_task
