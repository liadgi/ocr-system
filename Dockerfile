FROM java:8
COPY output/artifacts/LocalApp_jar/LocalApp.jar /home/LocalApp.jar
COPY bigFile.txt /home/bigFile.txt 
EXPOSE 8080
CMD ["java", "-jar", "/home/LocalApp.jar", "/home/bigFile.txt", "200"

# set region
# set dependencies (AWS SDK)
# credentials