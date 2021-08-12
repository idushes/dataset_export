FROM python:3.9.4

COPY requirements.txt /
RUN pip3 install -r requirements.txt

COPY src/ /app
WORKDIR /app

CMD ["python3", "/app/main.py"]