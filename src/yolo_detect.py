from ultralytics import YOLO
import os
import csv

model = YOLO("yolov8n.pt")

IMAGE_DIR = "data/raw/images"
OUTPUT = "data/yolo_results.csv"

rows = []

for channel in os.listdir(IMAGE_DIR):
    folder = os.path.join(IMAGE_DIR, channel)

    for img in os.listdir(folder):

        path = os.path.join(folder, img)
        results = model(path)

        for r in results:
            for box in r.boxes:
                cls = model.names[int(box.cls)]
                conf = float(box.conf)

                rows.append([img.split(".")[0], cls, conf])

with open(OUTPUT, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["message_id", "object", "confidence"])
    writer.writerows(rows)
