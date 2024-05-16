# etl_challenge

# Data Engineering Challenge 

[![Python Version](https://img.shields.io/badge/python-3.8%20%7C%203.9-blue)](https://www.python.org/downloads/)

## Description

To analyze the provided dataset using SQL queries, I address each of the given questions:

a)
- Identify the top 10 users based on the number of songs listened to.
- Determine the number of users who listened to some song on the 1st of March 2019.
- Find the first song each user listened to.
  
b)
For each user, determine the top 3 days with the most listens and the corresponding number of listens on each of these days.

c) [Optional] Calculate the absolute number of active users and the percentage of active users among all users on a daily basis, considering a user as active if they listened to at least one song within the past 6 days.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)

## Installation

Below are given the steps needed to get the project up and running. Initially, we need to install packages necessary for the normal run of the project. 

```bash
pip install -r requirements.txt
```

##Usage

Initially, we need to build the database along with table.

```bash
python3 DDL.py
```

Secondly, we need to run the ETL pipeline to extract the data from the dataset, perform a number of transformations and, finally, load to the target table. In order for the ETL pipeline to run, the dataset should be located in the following path './dataset/dataset.txt'. A log file (etl.log) will be generated in the working directory.  

```bash
python3 ETL.py
```

Finally, we can run the SQL queries to address all analysis as specified in the assignment description. 

```bash
python3 DATA_ANALYSIS.py
```
