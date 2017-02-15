#!/bin/bash
SESSION=de-ny-karthik-spark
ID=1
tmux new-session -s $SESSION -n bash -d
tmux new-window -t $ID
tmux send-keys -t $SESSION:$ID 'PYSPARK_PYTHON=/home/ubuntu/miniconda3/bin/python3.5 spark-submit --master spark://ip-xx-xx-x-xx:7077 --jars /home/ubuntu/Insight-Project/Stream_Spark/spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar /home/ubuntu/Insight-Project/Stream_Spark/Trades.py '"$ID"'' C-m
