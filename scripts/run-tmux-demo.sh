#!/bin/bash

# Ensure tmux is installed
if ! command -v tmux &> /dev/null; then
    echo "tmux is not installed. Please install it to run the split-screen demo (e.g. 'brew install tmux')."
    exit 1
fi

echo "Starting Kairo Demo in tmux..."
echo "Top pane: Docker Compose Logs (showing failover routing in real-time)"
echo "Bottom pane: Load Generator live metrics"
echo "Press Ctrl+C in the tmux session to exit once the test is complete."
sleep 3

# Kill any existing demo session
tmux kill-session -t kairo-demo 2>/dev/null

# Create a new session detached
# The top pane will tail the docker logs, filtering for important routing events
tmux new-session -d -s kairo-demo 'docker compose logs -f | grep -E "ROUTING-FAILOVER|CLUSTER-HEALTH"'

# Split the window vertically, taking up 50% of the screen
tmux split-window -v -p 50 -t kairo-demo './scripts/combined-test.sh'

# Attach to the session
tmux attach -t kairo-demo
