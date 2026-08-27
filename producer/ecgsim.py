import json
import os
import time
import wfdb
from kafka import KafkaProducer
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "ecg.raw"
DB_DIR = "../mitbh"
SAMPLE_RATE = 360
WINDOW_SIZE = SAMPLE_RATE
TEST_WINDOWS = 10

def main():
    record_names = []
    for filename in os.listdir(DB_DIR):
        if filename.endswith(".hea"):
            record_names.append(filename[:-4])
    record_names.sort()
    print("Records found:", record_names)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        linger_ms=5,
    )
    interval = 1.0
    try:
        for record_name in record_names:
            record = wfdb.rdrecord(
                os.path.join(DB_DIR, record_name)
            )
            n_samples, n_leads = record.p_signal.shape
            lead_names = record.sig_name
            print(
                f"\nStreaming record {record_name}"
            )
            print(
                f"Samples: {n_samples} | "
                f"Leads: {n_leads} | "
                f"Sample rate: {SAMPLE_RATE} Hz"
            )
            print(
                f"Full duration: "
                f"{n_samples / SAMPLE_RATE:.1f} seconds"
            )
            # Stream only the first 10 one-second windows
            for window_number in range(TEST_WINDOWS):
                start = window_number * WINDOW_SIZE
                if start >= n_samples:
                    break
                end = min(
                    start + WINDOW_SIZE,
                    n_samples
                )
                window = record.p_signal[start:end, :]
                payload = {
                    "record": record_name,
                    "timestamp": start / SAMPLE_RATE,
                    "start_sample": start,
                    "sampling_rate": SAMPLE_RATE,
                    "leads": {
                        lead_names[j]: window[:, j].tolist()
                        for j in range(n_leads)
                    }
                }
                producer.send(
                    TOPIC,
                    key=f"{record_name}:{start}",
                    value=payload
                )
                print(
                    f"  -> window {window_number + 1}/{TEST_WINDOWS} "
                    f"| samples {start}-{end - 1} "
                    f"| timestamp {start / SAMPLE_RATE:.0f}s"
                )
                time.sleep(interval)
            print(
                f"Finished test stream for record {record_name}"
            )
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        producer.flush()
        producer.close()
        print("Done")

if __name__ == "__main__":
    main()