#This is a userdefined file for the log
from datetime import datetime
def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("log_file.txt", "a") as f:
        f.write(f"{timestamp} - {message}\n")