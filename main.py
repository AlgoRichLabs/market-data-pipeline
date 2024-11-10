import os
from dotenv import load_dotenv
import databento as db

# Establish connection and authenticate
load_dotenv(".env")
client = db.Historical(os.getenv('API_KEY'))

# Authenticated request
print(client.metadata.list_datasets())