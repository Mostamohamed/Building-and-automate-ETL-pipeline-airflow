# Building-and-automate-ETL-pipeline-airflow
building an Airflow DAG using Astro (Astronomer) with Docker to automate the extraction, transformation, and delivery of Portland weather data.

### 🧰 **Tools and Environment**

* **Airflow**: Orchestrating tasks.
* **Astro CLI with Docker**: Running Airflow locally in containers.
* **Python**: Used for custom logic (data transformation and Discord integration).
* **HTTP API (OpenWeatherMap)**: Source of weather data.
* **Discord API**: Destination for sending the resulting CSV file.

---

### 📦 **Workflow Summary**

DAG (`weather_dag`) runs **daily** and automates the following steps:

---

### 1. **Sensor: Check Weather API Availability**

**Task:** `is_weather_api_ready`

* Uses `HttpSensor` to **ping the OpenWeatherMap API**.
* Proceeds only if the API is reachable.

---

### 2. **Extract: Fetch Current Weather Data**

**Task:** `extract_weather_data`

* Uses `HttpOperator` to **fetch weather data for Portland**.
* Parses the JSON response and makes it available via **XCom**.

---

### 3. **Transform & Load: Process Data and Save as CSV**

**Task:** `transform_load_weather_data`

* A `PythonOperator` that:

  * Pulls raw JSON data from XCom.
  * Converts temperature from **Kelvin to Fahrenheit**.
  * Extracts and formats weather fields (e.g., humidity, wind, sunrise/sunset).
  * Saves the processed data as a CSV in `/usr/local/airflow/output/`.

---

### 4. **Delivery: Send CSV to Discord**

**Task:** `send_csv_to_discord`

* Another `PythonOperator` that:

  * Retrieves the CSV file path from XCom.
  * Sends it to a **specific Discord channel** using a bot and `requests.post()` to the Discord API.

---

### 🔁 **Task Dependencies**

```text
is_weather_api_ready 
        ↓
extract_weather_data 
        ↓
transform_load_weather_data 
        ↓
send_csv_to_discord
```

---

### 🛠️ **Additional Notes**

* Uses a Discord bot token and channel ID (consider moving these to **Airflow Variables or Secrets** for security).
* Output file names are timestamped to avoid overwriting.
* DAG is robust with retries and scheduled to run **daily** with `catchup=False`.

---

### ✅ **Outcome**

Every day, the pipeline will:

1. Check if the weather API is live.
2. Fetch the current weather in Portland.
3. Format the data and store it in a timestamped CSV.
4. Send that CSV to a Discord channel automatically.

Let me know if you want help improving security (e.g., hiding the bot token), modularizing the code, or extending this DAG for multiple cities.
