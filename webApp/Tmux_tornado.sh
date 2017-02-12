#!/bin/bash
SESSION=de-ny-karthik-web
ID=1
tmux new-session -s $SESSION -n bash -d
tmux new-window -t $ID
tmux send-keys -t $SESSION:$ID 'sudo -E python tornadoapp.py '"$ID"'' C-m

