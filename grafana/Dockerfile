FROM grafana/grafana:7.3.6-ubuntu

USER grafana
#USER 472

SHELL ["/bin/bash","-c"]

#RUN grafana-cli plugins install frser-sqlite-datasource
#RUN wget https://github.com/fr-ser/grafana-sqlite-datasource/releases/download/v0.2.7/frser-sqlite-datasource-0.2.7.zip && \
#    unzip frser-sqlite-datasource-0.2.7.zip -d my-plugins/frser-sqlite-datasource

#VOLUME sq /sq
COPY graf.db /var/lib/plugins/graf.db

ENTRYPOINT ["/run.sh"]
