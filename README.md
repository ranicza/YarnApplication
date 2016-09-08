#Yarn application

- YARN Applications, which reads all the links from file (Destination URL field), crawl the pages and tag them (split into word all text fields and group them, count the most recent words). Full fill 'Keyword Value' field with 10 most popular tags;
- If URL is not available - extracts words from the link;
- Several containers partition is implemented;

## Build
``` 
mvn clean install

```
## How to run
```
cp ~/target/yarn-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar <place_to_save>

cd <place_to_save>

yarn jar  <place_to_save>/yarn-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar  com.epam.bigdata.q3.task2.yarn_app.Client -jar   <place_to_save>/yarn-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar  -num_containers=<number_of_containers>   
```

### Example
```
cp /root/eclipse/workspace/yarn-app/target/yarn-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar /usr/hdp/2.4.0.0-169/hadoop-yarn 

cd /usr/hdp/2.4.0.0-169/hadoop-yarn 

yarn jar  /usr/hdp/2.4.0.0-169/hadoop-yarn/yarn-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar  com.epam.bigdata.q3.task2.yarn_app.Client -jar   /usr/hdp/2.4.0.0-169/hadoop-yarn/yarn-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar  -num_containers=3 
```