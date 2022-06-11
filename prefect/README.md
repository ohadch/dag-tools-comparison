# Prefect Showcase

# Setup

## Set up the environment
```bash

# Install Prefect
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

# Start the server with custom ports
prefect server start --postgres-port 5435 --ui-port 8083

# Start the local agent (in a separate terminal)
python agents/local.py

# Create a new project
python projects/tutorial.py

# Register the flow
python flows/airflow_tutorial_flow.py
```

## Run the flow
- Open the UI at `http://localhost:8083/`
- Go to the `Flows` tab
- Click on the `airflow_tutorial_flow` flow
- Trigger the flow