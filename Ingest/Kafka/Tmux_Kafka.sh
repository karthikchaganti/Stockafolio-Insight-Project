#!/bin/bash
IP_ADDR=172.31.0.100
SESSION=de-ny-karthik-kafka
ID=1
tmux new-session -s $SESSION -n bash -d
tmux new-window -t $ID
tmux send-keys -t $SESSION:$ID 'python kafka_Producer.py '"$IP_ADDR"' '"$ID"'' C-m



