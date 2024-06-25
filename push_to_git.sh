#!/bin/bash

# Navigate to the right directory
cd /Users/maxrood/werk/greit/klanten/finn_it/intern

# Stage all changes
git add .

# Commit the changes with a timestamp
git commit -m "Update $(date +"%Y-%m-%d %H:%M:%S")"

# Push the changes
git push origin main

# Show a message when successfull
echo "Wijzigingen zijn succesvol gepusht naar GitHub."


