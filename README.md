# Server-Log-Data-Extraction-and-User-History-Database-Update
Server Log Data Extraction and User History Database Update
# Server Log Data Extraction and User History Database Update

## 📌 Project Overview

This project is a full ETL (Extract, Transform, Load) pipeline designed to process server log data from `mbox.txt`. It extracts email addresses and their associated timestamps, transforms the data into a standard format, and loads it into both MongoDB (for staging) and MySQL (for analysis). The final database is then queried to extract useful insights into user email activity.

---

## 🛠️ Technologies Used

- **Python** (Core scripting)
- **Regular Expressions** (Data extraction)
- **MongoDB** (NoSQL staging database)
- **MySQL / SQLite** (Relational analysis database)
- **Pandas** (For tabular representation of query outputs)
- **Logging** (Custom logging module)

---

## 📁 Dataset

- `mbox.txt` — Standard server log file containing raw email data and timestamps.

---

## 🎯 Project Objectives

1. **Extract**:
   - Identify all email addresses using regex.
   - Extract corresponding timestamps.

2. **Transform**:
   - Convert extracted timestamps to `UTC` in the format `YYYY-MM-DD HH:MM:SS`.

3. **Load**:
   - Insert the cleaned data into MongoDB (`user_history` collection).
   - Fetch the staged data and insert into MySQL or SQLite (`user_history` table).

4. **Analyze**:
   - Run SQL queries to gain insights:
     - Unique email addresses
     - Email count per day
     - First and last appearance of each email
     - Count of emails per domain (e.g., gmail.com)

---
