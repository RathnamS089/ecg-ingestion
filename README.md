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

##Project structure:
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
