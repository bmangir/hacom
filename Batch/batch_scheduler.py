import schedule
import time
from datetime import datetime
import logging
import sys
from dotenv import load_dotenv
import os

from Batch.UserBatchProcess.user_batch_stream_engine import UserBatchService
from Batch.ItemBatchProcess.item_batch_stream_engine import ItemBatchService


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors"""
    
    COLORS = {
        'INFO': '\033[92m',  # Green
        'WARNING': '\033[93m',  # Yellow
        'ERROR': '\033[91m',  # Red
        'DEBUG': '\033[94m',  # Blue
        'CRITICAL': '\033[95m',  # Purple
        'RESET': '\033[0m'  # Reset color
    }

    def format(self, record):
        # Add color to the level name
        if record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}{self.COLORS['RESET']}"
        return super().format(record)


# Configure logging
logger = logging.getLogger('BatchScheduler')
logger.setLevel(logging.INFO)

# File handler
file_handler = logging.FileHandler('/var/log/spark/batch_scheduler.log')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

# Console handler with colors
console_handler = logging.StreamHandler(sys.stdout)
colored_formatter = ColoredFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(colored_formatter)
logger.addHandler(console_handler)


def run_batch_processes():
    """Run all batch processes"""
    try:
        logger.info("="*50)
        logger.info("Starting batch processes at %s", datetime.now())
        logger.info("="*50)

        # Run User Batch Process
        logger.info("Starting User Batch Process")
        print("Starting User Batch Process")
        user_batch = UserBatchService()
        user_batch.start()
        print("User Batch Process completed successfully!")

        # Run Item Batch Process
        logger.info("Starting Item Batch Process")
        print("Starting Item Batch Process")
        item_batch = ItemBatchService()
        item_batch.start()
        print("Item Batch Process completed successfully!")

        logger.info("="*50)
        logger.info("All batch processes completed successfully!")
        logger.info("="*50)
    except Exception as e:
        logger.error("Error in batch processes: %s", str(e), exc_info=True)
        pass


def main():
    logger.info("Batch Scheduler started")

    # Schedule the job to run at 2 AM every day
    schedule.every().day.at("02:00").do(run_batch_processes)
    
    # Run the job immediately on startup
    run_batch_processes()
    
    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main() 