# ecg-ingestion

A real-time ECG data ingestion pipeline that streams ECG records through Apache Kafka, processes the data using Node-RED, and stores the resulting ECG samples in InfluxDB for visualization and further analysis.

## Architecture

```text
MIT-BIH ECG Dataset
        |
        v
  Python Producer
        |
        v
      Kafka
        |
        v
    Node-RED
        |
        v
    InfluxDB
        |
        v
ECG Visualization
```

##Project Structure

``` text
ecg-ingestion/
│
├── compose.yaml
├── requirements.txt
├── readme.md
│
├── producer/
│   └── ecgsim.py
│
├── nodered/
│   └── Node-RED flow/configuration
│
├── notebooks/
│   └── Analysis and experimentation notebooks
│
└── mitbh/
    └── MIT-BIH ECG data
```
## How to Run

### 1. Clone the Repository

Clone the repository and move into the project directory:

```bash
git clone https://github.com/RathnamS089/ecg-ingestion.git
cd ecg-ingestion
```

### 2. Create and Activate a Python Virtual Environment

Create a virtual environment:

```bash
python3 -m venv venv
```

Activate it:

**macOS / Linux:**

```bash
source venv/bin/activate
```

**Windows:**

```bash
venv\Scripts\activate
```

### 3. Install Python Dependencies

Install the required Python packages:

```bash
pip install -r requirements.txt
```

### 4. Start Kafka, Node-RED and InfluxDB

The project uses Docker Compose to run the required services.

Start all services in the background:

```bash
docker compose up -d
```

Check that the containers are running:

```bash
docker compose ps
```

The following services should be running:

```text
Kafka
Node-RED
InfluxDB
```

### 5. Verify InfluxDB

InfluxDB runs on port `8086`.

Check its health using:

```bash
curl http://localhost:8086/health
```

A successful response should contain:

```json
{
  "status": "pass"
}
```

InfluxDB can also be accessed through the web interface:

```text
http://localhost:8086
```

The initial configuration uses:

```text
Username: admin
Organization: ecg
Bucket: ecg_data
```

> If credentials are configured through environment variables, use the values defined in your local `.env` file.

### 6. Open Node-RED

Node-RED runs on port `1880`.

Open:

```text
http://localhost:1880
```

The Node-RED flow is responsible for consuming ECG messages from Kafka, parsing the JSON payload, extracting the ECG leads, and forwarding the samples to InfluxDB.

Make sure the flow is deployed before starting the ECG producer.

The expected flow is:

```text
Kafka Consumer
      |
      v
Function
      |
      v
InfluxDB
```

### 7. Start the ECG Producer

The Python producer streams ECG data from the MIT-BIH dataset into Kafka.

From the project root:

```bash
python3 producer/ecgsim.py
```

The producer divides the ECG signal into windows and publishes them to Kafka.

The ECG data is sampled at:

```text
360 Hz
```

Therefore, a one-second window contains:

```text
360 samples
```

The producer output will indicate the records/windows being streamed.

Example:

```text
Streaming record ...
Samples: ...
Leads: ...
Sample rate: 360 Hz
```

### 8. Verify the Data in Node-RED

While the producer is running, open Node-RED:

```text
http://localhost:1880
```

The Debug nodes can be used to verify that ECG messages are being received.

The processing function should report the available leads and number of samples, for example:

```text
LEADS: MLII, V2
Processing MLII | samples: 360
Processing V2 | samples: 360
```

### 9. Verify the Data in InfluxDB

Open:

```text
http://localhost:8086
```

Go to:

**Data Explorer**

Select:

```text
Bucket: ecg_data
Measurement: ecg
Field: value
```

ECG leads can be selected using the `lead` tag.

For example:

```text
lead = MLII
```

The stored samples can be viewed as raw data or plotted as a time-series waveform.

### 10. Stop the Services

When finished, stop the Docker services with:

```bash
docker compose down
```

This stops the containers while preserving the Docker volumes.

To stop the services and remove the associated volumes:

```bash
docker compose down -v
```

> **Warning:** `docker compose down -v` removes the Docker volumes, including the stored InfluxDB data. Use it only when you want to reset the database.

## Complete Startup Sequence

After cloning the repository, the typical startup sequence is:

```bash
# Clone the project
git clone https://github.com/RathnamS089/ecg-ingestion.git
cd ecg-ingestion

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Start Docker services
docker compose up -d

# Check services
docker compose ps

# Start ECG streaming
python3 producer/ecgsim.py
```

Then open:

```text
Node-RED:  http://localhost:1880
InfluxDB:  http://localhost:8086
```

## Data Flow During Execution

Once everything is running, ECG data moves through the system as follows:

```text
MIT-BIH ECG Dataset
        |
        v
  Python Producer
        |
        | ECG windows
        v
      Kafka
        |
        | JSON messages
        v
    Node-RED
        |
        | Parsed ECG samples
        v
    InfluxDB
        |
        | Time-series queries
        v
 ECG Visualization
```


