read -p "IP Address: " ipAddr
ipAddr=${ipAddr}

SESSION='de-ny-karthik-kafka'

# New tmux session
tmux new-session -s $SESSION -n bash -d

# Create a new window for electricity data
ElecID=1
tmux new-window -t $ElecID
tmux send-keys -t $SESSION:$ElecID 'python kafka_Producer.py '"$ipAddr"'' C-m
