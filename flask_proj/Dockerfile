FROM python:3

MAINTAINER Joanthan Deissler

COPY app /app

WORKDIR /app

RUN pip install -r requirements.txt

CMD ["python","-u", "app.py"]