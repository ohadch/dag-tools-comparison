# Dagster Showcase

## Setup
### Set up the Environment
```bash
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Run the Server
```bash
dagit -p 3001 -f jobs/hello_cereal.py
```

### Trigger the Job
- Go to the UI at `http://localhost:3001/`
- Go to the `Launchpad` tab
- Trigger the job