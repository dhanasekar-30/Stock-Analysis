import os

# Directories
currentPath = os.path.dirname(os.path.abspath(__file__))
csv_path = currentPath+"\data\symbol_metadata.csv"
dataFolder = currentPath+"\data"

# Field names 
highPrice = 'high'
lowPrice = 'low'
openPrice = 'open'
closedPrice = 'close'
timestamp = 'timestamp'
volume = 'volume'