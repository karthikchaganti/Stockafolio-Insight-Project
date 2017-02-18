
# Stockafolio
## Insight Data Engineering
- Website: http://insightdata.karthikchaganti.com/
- Presentation: https://goo.gl/DJHBbe

## The Data Pipeline
![Alt text](/Data-Pipeline)

### Motivation
- Back in the early 2000s, the stock portfolio updation was very slow. But today, instantaneous updation is vital
as quick decisions make a faster and profiting trade. That is what I tried to replicate in this project.

### What is Stock-a-folio?
- A highly scalable, low-latent and always available stock portfolio managmeent application.
- It can handle 10 million users and a throughput of 20,000 trades per second. Yet the portfolio is updated in the blink of an eye.
- Built using all distributed open-source technologies.

### The Stack
 - Built on Amazon Webservices EC2


| Tecnology     | Purpose       |
| ------------- | ------------- |
| Python  | Data Generator  |
| Apache Kafka  | Ingestion  |
| Apache Spark  | Batch Processing Engine  |
| Apache Spark Streaming | Stream Processing Engine |
| Apache Cassandra  | Data Storage  |
| Python Flask  | Server-side Web App  |
| Hadoop HDFS | Batch File System  |

### Data Schema
- Data is engineered using original S&P 500 stocks and then gaussian distribution is applied to it to simulate change in every second. The script is written in python.
- In order to simulate high scalability, a total of 10 million users are generated who keep trading on 1-500+ of these stocks randomly.




[![Analytics](https://ga-beacon.appspot.com/UA-92170532-1/https://github.com/karthikchaganti/Stockafolio-Insight-Project)](https://github.com/igrigorik/ga-beacon)
