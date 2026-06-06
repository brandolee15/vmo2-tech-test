# VMO2 Data engineer tech test 

## Overview
This repo contains the solution to the VMO2 Data Engineer tech test. The goal is to write a Python program by building an Apache Beam batch data pipeline that processes a transaction CSV file. 

## Task requirements

### Task 1
Write an Apache Beam batch job in Python satisfying the following requirements. 
- Find all transactions that have a transaction.amount greater than 20
- Exclude all transactions made before the year 2010
- Sum the total by date
- Save the output into output/results.jsonl.gz
- Make sure all files in output/ directory is git ignored

### Task 2
- Group all transform steps into a single Composite Transform
- Add a unit test to the Composite Transform using tooling/libraries provided by Apache Beam

## Prerequisites
Before running this project, make sure you have the following installed on your local machine:
- Python 3.11 or higher
- Git
- pip

## Getting started

### 1. Clone the repo
```bash
git clone https://github.com/brandolee15/vmo2-tech-test.git
cd vmo2-tech-test
```

### 2. Download the transactiond data
```bash
curl "https://storage.googleapis.com/cloud-samples-data/bigquery/sample-transactions/transactions.csv" -o transactions.csv
```

### 3. Create and activate a virtual environment 
**Mac/Linux:**
```bash
python -m venv venv
source venv/bin/activate
```

**Windows (Git Bash):**
```bash
python -m venv venv
source venv/Scripts/activate
```

### 4. Install dependencies
```bash
pip install -r requirements.txt
```

### 5. Run the pipeline
```bash
python main.py
```

The output file will be saved to `output/results.jsonl.gz`.

### 6. Run the tests
```bash
python -m pytest test_main.py -v
```

## Project Structure
```
vmo2-tech-test/
├── main.py              # Apache Beam pipeline
├── test_main.py         # Unit tests
├── requirements.txt     # Project dependencies
├── transactions.csv     # Input data (not committed to Git)
├── .gitignore           
└── output/              # Pipeline output (not committed to Git)
```