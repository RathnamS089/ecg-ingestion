import json
import os
import time
import wfdb
from kafka import KafkaProducer
from datetime import datetime

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "ecg.raw"
DB_DIR = "../mitbh"
RECORD_NAME = "100"
SAMPLE_RATE = 360
WINDOW_SIZE = SAMPLE_RATE          # 360 samples = 1 second
TOTAL_WINDOWS = 360                # 360 seconds of data


def main():
    record_path = os.path.join(DB_DIR, RECORD_NAME)
    record = wfdb.rdrecord(record_path)
    n_samples, n_leads = record.p_signal.shape
    lead_names = record.sig_name

    print(f"Record       : {RECORD_NAME}")
    print(f"Samples      : {n_samples} | Leads: {n_leads} | Rate: {SAMPLE_RATE} Hz")
    print(f"Full duration: {n_samples / SAMPLE_RATE:.1f}s")
    print(f"Streaming    : {TOTAL_WINDOWS} windows (0-{TOTAL_WINDOWS - 1}s)\n")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        linger_ms=5,
    )

    basetime = int(time.time() * 1000)

    try:
        for win in range(TOTAL_WINDOWS):
            start = win * WINDOW_SIZE
            if start >= n_samples:
                print(f"Reached end of record at window {win}")
                break

            end = min(start + WINDOW_SIZE, n_samples)
            window_ts = basetime + int((start / SAMPLE_RATE) * 1000)
            window = record.p_signal[start:end, :]

            payload = {
                "record": RECORD_NAME,
                "timestamp": window_ts,
                "start_sample": start,
                "sampling_rate": SAMPLE_RATE,
                "leads": {
                    lead_names[j]: window[:, j].tolist()
                    for j in range(n_leads)
                },
            }

            producer.send(
                TOPIC,
                key=f"{RECORD_NAME}:{start}",
                value=payload,
            )

            ts_str = datetime.fromtimestamp(window_ts / 1000).strftime("%H:%M:%S")
            print(
                f"  [{win + 1:3d}/{TOTAL_WINDOWS}] "
                f"samples {start:>6d}-{end - 1:<6d} | "
                f"t={start / SAMPLE_RATE:>5.0f}s | {ts_str}"
            )
            time.sleep(1)

        print(f"\n✅ Finished streaming record {RECORD_NAME}")
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        producer.flush()
        producer.close()
        print("Done")


if __name__ == "__main__":
    main()