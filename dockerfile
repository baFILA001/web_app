FROM python:3.9

WORKDIR /app

RUN pip install streamlit pandas google-cloud-bigquery vertexai google-cloud-secret-manager  

VOLUME /app/credentials

COPY . .

CMD ["streamlit", "run", "practice_03.py"]
