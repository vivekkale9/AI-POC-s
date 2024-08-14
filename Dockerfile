FROM python:3.9-slim-buster

WORKDIR /app

COPY . /app

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean

RUN pip install --no-cache-dir -r requirements.txt

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

EXPOSE 5000

ENV MONGO_URI=mongodb+srv://root:root@learningmongo.cr2lsf3.mongodb.net
ENV DATABASE=mRounds
ENV SPARK_JARS_PACKAGES=org.mongodb.spark:mongo-spark-connector_2.12:3.0.1
ENV SPARK_DRIVER_HOST=localhost
ENV PORT=5000
ENV FLASK_ENV=development
ENV FLASK_APP=apache_plask

CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]
