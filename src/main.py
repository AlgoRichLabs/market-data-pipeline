import databento as db
import pandas as pd
from databento import BentoClientError
import read_env_var as env


def get_start_timestamp() -> pd.Timestamp:
    """
    Get the timestamp of the last event in the OHLCV hourly data file
    """
    # Open the file in binary mode and read the last line
    with open(env.ROOT_PATH + env.OHLCV_HOUR_PATH, 'rb') as file:
        # Move to the end of the file
        file.seek(0, 2)  # Move the pointer to the end of the file
        # Traverse backwards until we find a newline character
        while file.tell() > 0:
            file.seek(-2, 1)  # Move backward two bytes
            if file.read(1) == b'\n':  # Check for newline
                break
        # Read the last line
        last_line = file.readline().decode()
    # Extract the `ts_event` (index 0) value from the last line by splitting on commas
    return pd.Timestamp(last_line.strip().split(',')[0])


# Establish connection and authenticate
client = db.Historical(env.API_KEY)

# submit job
end_timestamp = pd.Timestamp.now(tz="UTC")
while True:
    try:
        job_details = client.batch.submit_job(
            dataset="XNAS.ITCH",
            symbols=["IWY"],
            schema="ohlcv-1h",
            encoding="csv",
            compression=None,
            start=get_start_timestamp(),
            end=end_timestamp,
        )
        break
    except BentoClientError as e:
        if e.json_body["detail"]["case"] == "data_end_after_available_end":
            end_timestamp = pd.Timestamp(e.json_body["detail"]["payload"]["available_end"])
        else:
            raise e

# get job id
job_id = job_details['id']

# get job files
files = client.batch.list_files(job_id=job_id)
target_file = [file["filename"] for file in files if file["filename"].startswith("xnas-itch")][0]

download_file_path = client.batch.download(
    job_id=job_id,
    output_dir="./",
    filename_to_download=target_file
)

