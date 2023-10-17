FROM amazonlinux
RUN yum update -y && yum upgrade -y
RUN yum install -y python3 awscli jq tar gzip make
WORKDIR /root/python
ADD ./python/requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY ./python .
COPY ./snowflake_code /root/snowflake_code
ENTRYPOINT ["python3", "src/wrapper.py"]