FROM ubuntu:18.04

#FROM python:3


RUN apt-get update && apt-get install -y firefox

RUN apt-get install -y python3

RUN apt-get install -y python3-pip

RUN apt-get install -y libpython2.7-stdlib

#RUN pip3 install pystrich

RUN pip3 install BeautifulSoup4

RUN pip3 install pandas

RUN pip3 install selenium

RUN pip3 install webdriver_manager

#RUN pip install json

WORKDIR /home

RUN mkdir csv/
RUN mkdir logs/
ADD ScrapeNewTokens.py /home/
ADD Logger.py /home/
ADD geckodriver /home/
#CMD ["ls"]

CMD ["python3", "./ScrapeNewTokens.py"]
