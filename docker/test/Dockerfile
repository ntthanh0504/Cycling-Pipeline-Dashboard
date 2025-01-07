FROM python:3.8

# install google chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get -y update
RUN apt-get install -y google-chrome-stable

# install pip and selenium
RUN pip install --upgrade pip
RUN pip install selenium==4.0.0
RUN pip install webdriver_manager

# set display port to avoid crash
ENV DISPLAY=:99

COPY . /app
WORKDIR /app

CMD ["python", "main.py"]
