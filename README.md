# Distributed Computing
**Using MapReduce to Analyze the Million Song Dataset**

This project focuses on using Apache Hadoop ( version 3.1.2 ) to develop MapReduce programs to analyze song summaries and metadata. [The Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/) was created under a grant from the National Science Foundation, project IIS-0713334. The original data was contributed by The Echo Nest, as part of an NSF-sponsored GOALI collaboration. A 280 GB set of records are stored in separate CSV files for this experiment. The file name consists of the type of data and an index. Each line in a file corresponds to a single song, with each field separated by commas.

Subset of features from the **analysis** files:

| song_id | song_hotness | sample_rate | duration | key | loudness | ... | tempo | segments_pitches |
|---------|--------------|-------------|----------|-----|----------|-----|-------|------------------|

Subset of features from the **metadata** files:

| artist_hotness | artist_id | artist_latitude | ... | song_id | title | similar_artists | year |
|----------------|-----------|-----------------|-----|---------|-------|-----------------|------|

## Analysis

There were various questions that come to mind when analyzing the dataset. I seek to answer these questions with MapReduce and various analytical approaches, including; histogramming, PageRank, ordinary learst squares for multiple linear regression, and visualization.

### Histogramming 

Traditional statistical analysis was performed to learn more about the data and the common trends of musical artists. 

1. Which artist(s) has the most songs in the data set?
    - Ike & Tina Turner, 208 songs
    - Johnny Cash, 199 songs
    - Diana Ross, 196 songs
2. Which artist(s) songs are the loudest on average?
    - Shenggy, 3.931 dB
    - Pens, 3.746 dB
    - Pain Jerk, 3.746 dB
3. Which artist(s) has the highest total time spent fading in their songs?
    - 50 Cent ft. Murda Mass Young Buck & Spider Loc, 40.5 minutes
    - Vincent Bruley, 39.1 minutes
    - Melvins, 37.7 minutes
4. What is the longest song(s)? The shortest song(s)? The song(s) of median length? The song(s) of mean length?
    - Longest: Grounation, 50.5 minutes
    - Shortest: Rainy Days And Mondays, 0.005 minutes
    - Median: Step Ya Game Up (Remix), 3.8 minutes
    - Mean: You Put a Spell on Me, 4.1 minutes
5. Create new segment data for the average song. Including start time, pitch, timbre, max loudness, max loudness time, and start loudness.
    - N/A for documentation purposes
    
### PageRank 



1. Which artist(s) is the most generic? Which artist(s) is the most unique?
    - Generic: The Rolling Stones, The Beatles, The Fairfield Four
    - Unique: Kuti, Brutal Deluxe, Lennox Brown
    
### Multi-Linear Regression

This method introduces use of multiple linear regression to fit a hyper plane to the sample space. Using ordinary least squares, we can approximate the values of parameters w to fit a model to the features that define a song. Then we can perform a search in the parameter space of w to find a solution of X which results in a hotness target T greater than the maximum in the data.

1. Create a song with a higher popularity score than that found in the dataset.      

**hotness** | duration | end_fade_in | key | loudness | mode | start_fade_out | tempo | time_sign 
---|---|---|---|---|---|---|---|---
**1.155** | 580.307 | 4.463 | 3.721 | -0.0871 | -1.293 | -518.386 | 171.054 | -0.318

### Visualization

1. What is the geographical density of artists corresponding to the year of song releases?
    - This explores visual and analytical components, and thus has been shown in a Jupyter Notebook [ here ](https://nbviewer.jupyter.org/github/stockeh/mapreduce-analysis-msd/blob/master/notebook/location-notebook.ipynb)
