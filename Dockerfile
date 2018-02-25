FROM python:3
RUN pip install boto3

COPY . .

CMD [ "python", "./main.py", "-v", "-p 1", "-c 1", "test_table" ]