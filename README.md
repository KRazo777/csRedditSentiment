# Sentiment Analysis on Computer Science Subreddits

## Overview
This project analyzes sentiment trends across popular computer science-related subreddits such as r/cscareerquestions, r/csmajors, and r/programming. The goal is to understand how topics like AI, jobs, and internships are discussed and how sentiment has changed over time.

We built a full data pipeline to collect, process, analyze, and visualize large-scale Reddit data, providing insights into trends within the tech community.

---

## Motivation
There has been growing anxiety around the tech job market, especially with the rise of AI and increasing competition. This project aims to quantify those concerns by analyzing real discussions from online communities.

We explored:
- Whether AI-related discussions tend to be negative  
- How sentiment around jobs and internships compares  
- How trends have evolved over time  

---

## System Architecture
The system follows a full data pipeline:

### Data Collection
- Collected Reddit posts and comments using PRAW (Reddit API)
- Targeted multiple CS-related subreddits
- Gathered ~550MB of data

### Data Processing
- Used Apache Spark (PySpark) for large-scale data processing
- Cleaned and filtered raw JSONL data
- Removed duplicates, empty entries, and irrelevant fields

### Storage
- Stored raw data in JSONL format
- Saved processed data in Parquet format
- Aggregated results into a MySQL database using JDBC

### Visualization
- Built an interactive dashboard using Streamlit
- Visualized sentiment trends and keyword-based insights

---

## Sentiment Analysis
We used **VADER (Valence Aware Dictionary and Sentiment Reasoner)** for sentiment scoring.

- Designed for social media and informal text  
- Outputs a compound score between -1 and +1  
- Positive: score > 0.05  
- Negative: score < 0.05  

---

## Key Findings
- Internship-related posts tend to have the most positive sentiment  
- Job-related discussions are the most negative  
- AI discussions have grown significantly over time  
- Sentiment around internships is positive but seasonal  

---

## Tech Stack
- **Languages:** Python  
- **Data Processing:** Apache Spark (PySpark)  
- **Data Collection:** PRAW (Reddit API)  
- **Database:** MySQL  
- **Visualization:** Streamlit  
- **NLP:** VADER Sentiment Analysis  

---

## Challenges
- Handling large-scale Reddit data (500MB+)  
- Dealing with deleted or incomplete posts  
- Capturing context (e.g., sarcasm) with rule-based sentiment models  

---

## Future Improvements
- Replace VADER with transformer-based models for better context understanding  
- Move infrastructure to cloud for scalability  
- Improve handling of sarcasm and nuanced sentiment  

---

## How to Run
```bash
# Clone the repository
git clone https://github.com/your-username/your-repo-name.git

# Navigate into the project
cd your-repo-name

# Install dependencies
pip install -r requirements.txt

# Run the dashboard
streamlit run app.py
