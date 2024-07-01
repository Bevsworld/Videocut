Document: Understanding the Flow and Process of the Video Processing Script
Overview
This document explains the flow and functionality of the provided Python script, which is designed for processing video files, extracting clips, and uploading them to DigitalOcean Spaces. The script performs various tasks including database operations, video downloading and processing, and cloud storage management.

Key Components
Imports and Libraries:

The script utilizes various libraries such as requests, sqlalchemy, boto3, logging, datetime, sys, subprocess, tenacity, and schedule for different purposes like web requests, database interaction, logging, AWS S3 client configuration, command-line interface, retry mechanisms, and task scheduling.
Logging Configuration:

Configures logging to log messages to both a file (video_processing.log) and the console with different log levels for each.
Database Configuration:

Sets up the connection to a PostgreSQL database using SQLAlchemy and defines the table structure for riksdagen.
DigitalOcean Spaces Configuration:

Initializes a DigitalOcean Spaces client for cloud storage using credentials and endpoint details.
Database Session Creation:

Creates a scoped session for interacting with the database.
Main Functions and Their Flow
Retry Decorator:

Uses the tenacity library to retry database operations in case of specific exceptions (SQLAlchemyError, OperationalError).
Function: get_unprocessed_entry:

Fetches the first unprocessed entry from the database table riksdagen where uploadedtospaces is False.
Function: download_video:

Downloads a video from a given URL, displaying a progress bar, and saves it to the local filesystem.
Function: convert_to_seconds:

Converts a time string (HH:MM
or MM
) to seconds for easier processing of video segments.
Function: ffmpeg_extract_subclip:

Uses ffmpeg to extract a subclip from a video file based on start time and duration.
Function: process_video:

Processes a video file by extracting subclips based on a list of speaker times and names. Clips are saved to a specified output folder.
Function: upload_to_digitalocean:

Uploads a file to DigitalOcean Spaces, setting its access control to public-read.
Function: update_entry_to_uploaded:

Updates a database entry to mark it as processed (uploadedtospaces=True).
Function: cleanup_files:

Removes specified files from the local filesystem to clean up after processing.
Function: process_entries:

The main processing loop that:
Fetches unprocessed entries from the database.
Downloads the video file.
Processes the video into subclips.
Uploads the clips to DigitalOcean Spaces.
Updates the database entry to mark it as processed.
Cleans up local files.
Function: main:

Runs process_entries immediately.
Schedules process_entries to run every hour using the schedule library.
Continuously checks and runs any pending scheduled tasks.
Execution Flow
Initialization:

The script starts by configuring logging, setting up database connections, and initializing the DigitalOcean Spaces client.
Main Loop:

The main function initiates the process by calling process_entries.
It then schedules process_entries to run hourly and enters an infinite loop to keep the script running and checking for scheduled tasks.
Processing Entries:

process_entries fetches unprocessed entries from the database and processes each entry sequentially by downloading the video, extracting clips, uploading them, updating the database, and cleaning up local files.
Error Handling and Retries
The script uses the tenacity library to implement exponential backoff retries for database operations, ensuring robustness against transient errors.
Logs errors at each step to facilitate debugging and maintenance.
Scheduling
The schedule library ensures that process_entries runs at regular intervals (every hour) to check for and process new entries, enabling continuous operation without manual intervention.
This detailed explanation should help understand the workflow and processes involved in the script, from initialization to video processing and uploading, along with error handling and scheduling mechanisms.
