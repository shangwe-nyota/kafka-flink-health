Report 
Shangwe Nyota

Window strategy (size/type):
I used a 1-minute tumbling event-time window and grouped by patient_id, so all heart rate events for a given patient within each fixed 60-second window get aggregated together with no overlap. I went with tumbling windows because they’re straightforward and consistent, and they make sense for computing clean, periodic summaries of a patient’s heart rate over time.

Watermark / out-of-order delay settings
I used event-time processing with a 5-second watermark to handle out-of-order data. This essentially means that if events come in slightly late, they can still be included in the correct window as long as they arrive within 5 seconds of when they actually happened. I chose 5 seconds as a small buffer to handle minor delays without slowing down the pipeline too much.

How I tested the pipeline
I tested the pipeline in multiple stages. First, I verified the Kafka producer by consuming messages directly from the heart_rate_events topic. Then I implemented and tested parse_event and classify_window using the provided unit tests. After that, I ran the Flink job locally and confirmed that windowed alert messages were being written to the heart_rate_alerts topic. Finally, for the batch component, I exported alert messages to a JSON file and used Spark to aggregate and validate the results.

Known limitations
There are no known limitations within the scope of this assignment. One potential area for improvement beyond this scope would be experimenting with more dynamic watermark strategies.

Note:
I made some minor adjustments were made to imports and configuration to ensure compatibility with the local PyFlink environment (I ran locally instead of linuxlab)
These changes did not affect the core logic of the pipeline.