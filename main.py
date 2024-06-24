import requests
from sqlalchemy import create_engine, Table, MetaData, update
from sqlalchemy.orm import sessionmaker, scoped_session
import boto3
import os
import logging
from datetime import datetime
import sys
import subprocess
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from sqlalchemy.exc import SQLAlchemyError, PendingRollbackError, OperationalError
import schedule
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
logger = logging.getLogger()

# File handler
file_handler = logging.FileHandler('video_processing.log')
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s:%(message)s'))
logger.addHandler(file_handler)

# Console handler with different level
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(logging.Formatter('%(message)s'))
logger.addHandler(console_handler)

# Database credentials and connection setup
DATABASE_URL = "postgresql://retool:jr1cAFW3ZIwH@ep-tight-limit-a6uyk8mk.us-west-2.retooldb.com/retool?sslmode=require"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
metadata = MetaData()
metadata.bind = engine

# Define or load your table structure
riksdagen_table = Table('riksdagen', metadata, autoload_with=engine)

# DigitalOcean Spaces configuration
DO_SPACES_ACCESS_KEY = 'DO009U4RBZ8UJAVE8DPL'
DO_SPACES_SECRET_KEY = 'NEh7GbCufcqpWqtFc91qTsGtJAaV6nnGD8qaLkVm5kU'
DO_SPACES_ENDPOINT = 'https://fra1.digitaloceanspaces.com'
DO_SPACES_BUCKET = 'samladpolitik'

# Initialize DigitalOcean Spaces client
session = boto3.session.Session()
client = session.client('s3',
                        region_name='fra1',
                        endpoint_url=DO_SPACES_ENDPOINT,
                        aws_access_key_id=DO_SPACES_ACCESS_KEY,
                        aws_secret_access_key=DO_SPACES_SECRET_KEY)

# Create a database session
Session = scoped_session(sessionmaker(bind=engine))
db_session = Session()


@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5),
       retry=retry_if_exception_type((SQLAlchemyError, OperationalError)))
def get_unprocessed_entry():
    try:
        return db_session.query(riksdagen_table).filter_by(uploadedtospaces=False).first()
    except SQLAlchemyError as e:
        db_session.rollback()
        logger.error(f"Error fetching unprocessed entry: {e}")
        raise


def download_video(url, filename):
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024
    wrote = 0
    with open(filename, 'wb') as file:
        for data in response.iter_content(block_size):
            wrote = wrote + len(data)
            file.write(data)
            done = int(50 * wrote / total_size)
            sys.stdout.write(f"\r[{'=' * done}{' ' * (50 - done)}] {wrote / total_size:.2%}")
            sys.stdout.flush()
    sys.stdout.write("\n")
    logger.info(f"Downloaded {filename}")


def convert_to_seconds(time_str):
    try:
        time_obj = datetime.strptime(time_str, '%H:%M:%S')
        return time_obj.hour * 3600 + time_obj.minute * 60 + time_obj.second
    except ValueError:
        time_obj = datetime.strptime(time_str, '%M:%S')
        return time_obj.minute * 60 + time_obj.second


def ffmpeg_extract_subclip(input_file, start_time, end_time, targetname):
    command = [
        "ffmpeg", "-y",
        "-i", input_file,
        "-ss", str(start_time),
        "-to", str(end_time),
        "-c", "copy",
        "-map", "0:v:0",
        "-map", "0:a:0",
        targetname
    ]
    subprocess.run(command, check=True)


def process_video(video_path, speakerlist, output_folder):
    speakers = speakerlist  # speakerlist is already a dictionary
    previous_time = '00:00'
    count = {}

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for time, speaker in speakers.items():
        start_time = convert_to_seconds(previous_time)
        end_time = convert_to_seconds(time)
        speaker_key = speaker.replace(" ", "_").replace("(", "").replace(")", "")
        if speaker_key not in count:
            count[speaker_key] = 0
        count[speaker_key] += 1
        clip_filename = f"{output_folder}/{count[speaker_key]:02d}_{speaker_key}.mp4"
        ffmpeg_extract_subclip(video_path, start_time, end_time, clip_filename)
        logger.info(f"Processed clip: {clip_filename}")
        previous_time = time


def upload_to_digitalocean(filename, folder):
    try:
        with open(filename, 'rb') as file:
            client.put_object(
                Bucket=DO_SPACES_BUCKET,
                Key=f"{folder}/{os.path.basename(filename)}",
                Body=file,
                ACL='public-read'
            )
        logger.info(f"Uploaded {filename} to DigitalOceans Spaces {folder}")
    except Exception as e:
        logger.error(f"Failed to upload {filename} to DigitalOcean Space {folder}: {e}")


@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5),
       retry=retry_if_exception_type((SQLAlchemyError, PendingRollbackError, OperationalError)))
def update_entry_to_uploaded(entry_id):
    try:
        stmt = update(riksdagen_table).where(riksdagen_table.c.id == entry_id).values(uploadedtospaces=True)
        db_session.execute(stmt)
        db_session.commit()
    except (PendingRollbackError, OperationalError) as e:
        db_session.rollback()
        logger.error(f"Error updating entry: {e}")
        raise
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy error: {e}")
        db_session.rollback()
        raise


def cleanup_files(files):
    for file in files:
        try:
            os.remove(file)
            logger.info(f"Removed file: {file}")
        except Exception as e:
            logger.error(f"Error removing file {file}: {e}")


def process_entries():
    while True:
        try:
            entry = get_unprocessed_entry()
            if entry:
                logger.info(f"Processing entry: {entry.title}")
                download_link = entry.download
                spaces_folder = entry.spacesfolder
                video_filename = "video.mp4"

                download_video(download_link, video_filename)
                process_video(video_filename, entry.speakerlist, spaces_folder)

                for file in os.listdir(spaces_folder):
                    upload_to_digitalocean(os.path.join(spaces_folder, file), spaces_folder)

                update_entry_to_uploaded(entry.id)
                logger.info(f"Entry {entry.title} processed and uploaded.")

                # Clean up files
                cleanup_files([video_filename])
                for file in os.listdir(spaces_folder):
                    cleanup_files([os.path.join(spaces_folder, file)])
            else:
                logger.info("No  unprocessed entries found.")
                break
        except Exception as e:
            logger.error(f"Error in processing entries: {e}")
            time.sleep(60)  # Wait for a minute before retrying


def main():
    process_entries()  # Run immediately
    schedule.every().hour.do(process_entries)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
