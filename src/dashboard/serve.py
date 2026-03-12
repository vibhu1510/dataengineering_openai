"""Wrapper to launch Streamlit from a fixed working directory."""
import os
import sys

os.chdir("/Users/vibhugupta/Documents/learning_projects/data_pipeline/openai")
sys.argv = ["streamlit", "run", "src/dashboard/app.py", "--server.port", "8501", "--server.headless", "true"]

from streamlit.web.cli import main
main()
