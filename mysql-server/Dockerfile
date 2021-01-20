FROM mysql/mysql-server:5.7

ENV MYSQL_ROOT_HOST=%

ENV MYSQL_ALLOW_EMPTY_PASSWORD=yes

ENV MYSQL_ROOT_PASSWORD=secret

ENV MYSQL_DATABASE=activus

ENV MYSQL_USER=azerty

ENV MYSQL_PASSWORD=azerty

VOLUME store_db /var/lib/mysql

#cd to the directory thar contains the current Dockerfile
# docker build -t NAME_YOU_WANT:TAG_YOU_WANT .
# docker run -dit -p 3306:3306 --name CONTAINER_NAME NAME_YOU_WANT:TAG_YOU_WANT
#NB: one must create an empty folder named store_db for having a successful volume mapping
