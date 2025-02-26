with open('04-analytics-engineering/downloads/fhv_tripdata_2020-02.csv', 'rb') as f:
    lines = f.readlines()
    for i, line in enumerate(lines):
        try:
            line.decode('utf-8')  # Try decoding each line
        except UnicodeDecodeError:
            print(f"Error at line {i+1}")
            break
