# Distributed Computing
**Using Mapreduce to Analyze the Million Song Dataset**

This project focuses on using Apache Hadoop ( version 3.1.2 ) to develop MapReduce programs that parse and process song analyses and metadata to answer related questions about the songs and artist.

[The Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/) was created under a grant from the National Science Foundation, project IIS-0713334. The original data was contributed by The Echo Nest, as part of an NSF-sponsored GOALI collaboration. A set of 280 GB of records are stored in separate CSV files for this experiment. The file name consists of the type of data and an index. Each line in a file corresponds to a single song, with each field separated by commas.

Subset of features from the **analysis** files:

| song_id | song_hotness | sample_rate | danceability | duration | key | loudness | ... | tempo | segments_pitches |
|---------|--------------|-------------|--------------|----------|-----|----------|-----|-------|------------------|

Subset of features from the **metadata** files:

| artist_fmiliarity | artist_hotness | artist_id | artist_latitude | ... | song_id | title | similar_artists | year |
|-------------------|----------------|-----------|-----------------|-----|---------|-------|-----------------|------|

## Analysis

There were various questions that come to mind when analyzing the dataset. I seek to answer these questions with MapReduce and versious analytical approaches, including; histograming, ordinary learst squares for multiple linear regression, PageRank, etc.

1. Which artist(s) has the most songs in the data set?
    - Ike & Tina Turner, 208 songs
    - Johnny Cash, 199 songs
    - Diana Ross, 196 songs
2. Which artist(s) songs are the loudest on average?
    - Shenggy, 3.931 dB
    - Pens, 3.746 dB
    - Pain Jerk, 3.746 dB
3. What is the song(s) with the highest popularity score?
    - Sleepyhead, 1.0
    - Halfway Gone, 1.0
    - Love Story, 1.0
4. Which artist has the highest total time spent fading in their songs?
    - 50 Cent ft. Murda Mass Young Buck & Spider Loc, 40.5 minutes
    - Vincent Bruley, 39.1 minutes
    - Melvins, 37.7 minutes
5. What is the longest song(s)? The shortest song(s)? The song(s) of median length? The song(s) of mean length?
    - Longest: Grounation, 50.5 minutes
    - Shortest: Rainy Days And Mondays, 0.005 minutes
    - Median: Step Ya Game Up (Remix), 3.8 minutes
    - Mean: You Put a Spell on Me, 4.1 minutes
6. Create *new* segment data for the average song. Including start time, pitch, timbre, max loudness, max loudness time, and start loudness.
    - ...
7. Which artist(s) is the most generic? Which artist(s) is the most unique?
    - Generic: The Rolling Stones, The Beatles, The Fairfield Four
    - Unique: Kuti, Brutal Deluxe, Lennox Brown
8. Create a song with a higher popularity score than that found in the dataset.  

**hotness** | duration | end_fade_in | key | loudness | mode | start_fade_out | tempo | time_sign 
---|---|---|---|---|---|---|---|---
**1.155** | 580.307 | 4.463 | 3.721 | -0.0871 | -1.293 | -518.386 | 171.054 | -0.318

These questions continue to evolve as a greater analysis is achieved...
