# 🎯 Opportunity Matcher System

A Big Data project that helps match students with suitable opportunities based on their skills, courses, major, and profile data.

This project was developed as a team project by Software Engineering students at An-Najah National University.

---

## 👥 Team Members

- Ahmad Istatieh
- Aprar Ismael
- Saja Dwaikat
- Adham Lahluh

---

## 🧠 Project Idea

The main idea of the system is to analyze student CV data and compare it with available opportunities in order to recommend the most suitable matches.

The system uses Big Data technologies to process, stream, store, and match data efficiently.

---

## 🚀 Features

- Student CV data processing
- Real-time data streaming using Kafka
- Data processing using Apache Spark
- Student-opportunity matching
- Similarity-based matching using LSH
- MongoDB database integration
- API support for retrieving matching results

---

## 🛠️ Tech Stack

- Scala
- Apache Kafka
- Apache Spark
- Spark Structured Streaming
- MongoDB
- Akka HTTP
- Git & GitHub

---

## 📂 Project Structure

```bash
opportunity-Matcher-/
│
├── src/                 # Main source code
├── project/             # Scala/SBT project files
├── build.sbt            # Project dependencies
├── README.md            # Project documentation
└── .gitignore
```

---

## 🔄 System Workflow

1. Student CV data is prepared and cleaned.
2. Data is sent to Kafka for streaming.
3. Spark reads the streamed data from Kafka.
4. Spark processes and transforms the data.
5. Processed data is stored in MongoDB.
6. LSH is used to compare student profiles with opportunities.
7. The system returns the best matching results.

---

## 🧩 Matching Approach

The system uses similarity-based matching to compare students with opportunities.

Student information such as:

- Skills
- Courses
- Major
- Training experience

is converted into meaningful tokens and then processed using LSH to find the closest matches.

---

## 📌 Project Goals

- Help students find suitable opportunities
- Apply Big Data concepts in a real project
- Use Kafka and Spark in a practical pipeline
- Practice teamwork and Git collaboration
- Build a scalable matching system

---

## 📊 What We Learned

Through this project, we practiced:

- Big Data pipeline design
- Kafka producer and consumer concepts
- Spark Structured Streaming
- MongoDB integration
- Similarity matching using LSH
- Team collaboration using GitHub

---

## 👨‍💻 Developed By

Software Engineering Students  
An-Najah National University

- Ahmad Istatieh
- Aprar Ismael
- Saja Dwaikat
- Adham Lahluh

---

## 🔗 Repository

```bash
https://github.com/ahmadistatieh/opportunity-Matcher-.git
```
