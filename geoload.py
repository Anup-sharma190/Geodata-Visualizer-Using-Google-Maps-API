"""
Project: Geodata Loader with SQLite and Google Maps API (Py4E Assignment)
Author: Anup Sharma
Skills: Python, SQLite, JSON, APIs, Data Persistence, Error Handling
Tools: urllib, json, sqlite3, ssl

Description:
------------
This program reads a list of locations from a file (`where.data`),
retrieves their geodata using the Google Maps Geocoding API (Py4E test server),
and stores the results in a local SQLite database (`geodata.sqlite`).

Steps Performed:
1. Connects to SQLite and creates a table `Locations`.
2. Reads location names from `where.data`.
3. For each location:
   - Checks if data is already in the database.
   - If not, retrieves JSON geodata from the API.
   - Stores the raw geodata in the database for future use.
4. Uses caching to avoid redundant API calls.
"""

import sqlite3
import urllib.request, urllib.parse, urllib.error
import json
import time
import ssl

# -------------------- Step 1: API & Service URL --------------------
api_key = False
serviceurl = 'https://py4e-data.dr-chuck.net/geojson?'  # Test API

# -------------------- Step 2: SSL Context --------------------
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# -------------------- Step 3: SQLite Setup --------------------
conn = sqlite3.connect('geodata.sqlite')
cur = conn.cursor()
cur.execute('''
CREATE TABLE IF NOT EXISTS Locations (address TEXT, geodata TEXT)''')

# -------------------- Step 4: Read Input File --------------------
fh = open("where.data")
for line in fh:
    address = line.strip()

    # Check if already in database
    cur.execute("SELECT geodata FROM Locations WHERE address= ?", (address,))
    try:
        data = cur.fetchone()[0]
        print("Found in database:", address)
        continue
    except:
        pass

    # Build API request
    parms = dict()
    parms["address"] = address
    parms["key"] = api_key if api_key else 42
    url = serviceurl + urllib.parse.urlencode(parms)

    print("Retrieving", url)
    uh = urllib.request.urlopen(url, context=ctx)
    data = uh.read().decode()
    print('Retrieved', len(data), 'characters', data[:40].replace('\n', ' '))

    try:
        js = json.loads(data)
    except:
        print("Error parsing JSON")
        continue

    if 'status' not in js or (js['status'] != 'OK' and js['status'] != 'ZERO_RESULTS'):
        print('Failure To Retrieve:', address)
        continue

    # Insert data into SQLite
    cur.execute('''INSERT INTO Locations (address, geodata)
            VALUES (?, ?)''', (addr
