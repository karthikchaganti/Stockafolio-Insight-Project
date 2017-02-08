SESSION='de-ny-karthik-sparkStreaming'

# New tmux session
tmux new-session -s $SESSION -n bash -d

# Create a new window for electricity data
ElecID=1
tmux new-window -t $ElecID
tmux send-keys -t $SESSION:$ElecID 'PYSPARK_PYTHON=/home/ubuntu/miniconda3/bin/python3.5 spark-submit --master spark://ip-172-31-0-100:7077 --jars spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar Trades.py' C-m
















