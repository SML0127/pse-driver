# Master of Distributed Web Crawler and Data Management System for Web Data 


## What we provide
- Crawl and Parse product data in distributed environment (Master / Worker).
- Update product information in the database incrementaly.

   (Update example)
<img width="400" height="500" alt="overall_architecture" src="https://user-images.githubusercontent.com/13589283/140600455-fc2c143e-9d12-4c8c-984f-e1d9b082c9fb.jpg">

- Upload crawled data to target sites.
- Register schedule for crawling and view maintenance ({upload, update} to {database, target sites}).

------------
## How to Impelement & Support
- Provide all services through GUI (based on React).
   - git repository link for Master: https://github.com/SML0127/pse-master-Dockerfile)
- For BFS Crawling Model, we provide several operators (based on Python).
- For supporting distributed environment, we used Redis & RQ as a Message Broker.
- [Docker](https://www.docker.com/) image for enviornment
   - git repository link for Master: https://github.com/SML0127/pse-master-Dockerfile)
   - git repository link for Worker: https://github.com/SML0127/pse-worker-Dockerfile)


------------
## What languages, libraries, and tools were used?
- Mainly based on Python
- Python Flask for Web Application Server and DB Server
- PostgreSQL for Database
- [Apachi Airflow](https://airflow.apache.org/) for Scheduling
- [Redis](https://redis.io/) & [RQ](https://python-rq.org/) for Message Broker in distributed environment
- [Selenium](https://www.selenium.dev/) & [Chromedriver](https://chromedriver.chromium.org/downloads) for Crawling
