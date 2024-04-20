FROM python:3.11.5

WORKDIR /opt
ADD main.py /opt
ADD requirements.txt /opt
ADD vsfetch /opt/vsfetch
RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3", "main.py"]
