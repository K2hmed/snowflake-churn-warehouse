import os
from dotenv import load_dotenv

load_dotenv()

print("ACCOUNT:", os.getenv("SNOWFLAKE_ACCOUNT"))
print("USER:", os.getenv("SNOWFLAKE_USER"))
