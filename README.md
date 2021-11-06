# Master of Distributed Web Crawler and Data Management System for Web Data 

## What we provide in Master

1. Crawl and Parse product data in distributed environment (Master / Worker).
2. Update product information in the database incrementaly.

   (Update example)
<img width="400" height="500" alt="overall_architecture" src="https://user-images.githubusercontent.com/13589283/140600455-fc2c143e-9d12-4c8c-984f-e1d9b082c9fb.jpg">


3. Upload crawled data to target sites.
4. Register schedule for crawling and view maintenance ({upload, update} to {database, target sites}).

------------
## How to Impelement & Support

0. Provide all services through GUI (based on React).
1. For BFS Crawling Model, we provide several operators (based on Python).
2. For supporting distributed environment, we used Redis & RQ as a Message Broker.
3. 

------------
## What languages, libraries, and tools were used?

- Mainly based on Python
- Python Flask for Web Application Server and DB Server
- PostgreSQL for Database
- Apachi Airflow for Scheduling
- Redis & RQ for Message Broker in distributed environment
- 

