#!/bin/bash
IP_ADDR=xxx.xx.xx.xx
SESSION=de-ny-karthik-kafka
ID=1
tmux new-session -s $SESSION -n bash -d
tmux new-window -t $ID
tmux send-keys -t $SESSION:$ID 'python kafka_Producer.py '"$IP_ADDR"' '"$ID"'' C-m
