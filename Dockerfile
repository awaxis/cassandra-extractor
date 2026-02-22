FROM python:3.7 AS v2

####################################################################
#  INSTALLING THE REQUIREMENTS (ONLY CONNECTORS AND PANDAS)	       #
####################################################################
RUN mkdir -p /opt/wadlabs/hub/migration/logs /opt/wadlabs/hub/migration/conf /etc/wadlabs
COPY requirements.txt /opt/wadlabs/hub/migration/
RUN python3 -m pip install --no-cache-dir -r /opt/wadlabs/hub/migration/requirements.txt

####################################################################
#  INSTALLING THE APPLICATION	                                   #
####################################################################
COPY ./wadlabs/hub/migration/*.py /opt/wadlabs/hub/migration/

WORKDIR /opt/wadlabs/hub/migration/

ENTRYPOINT python3 app.py