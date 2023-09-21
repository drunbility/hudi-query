FROM 10.0.10.244/bi-commons/openjdk:8-bi
RUN apk add --update font-adobe-100dpi ttf-dejavu fontconfig
ARG DOCKER_ENV
ENV server_env=${DOCKER_ENV}
ENV projectName=bi-report
WORKDIR /usr/local/lib/$projectName
COPY ./target/*.jar ./

ENTRYPOINT ["tini","java","-Xms1024m","-Xmx2048m","-Djava.awt.headless=true","-jar","bi-report-1.0-SNAPSHOT.jar"]
