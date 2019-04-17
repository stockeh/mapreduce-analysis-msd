————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————
—														       —
- 	                            Using MapReduce to Analyze the Million Song Dataset		      		       —
—														       —
—														       —
—				      Jason D Stock - stock - 830635765 - Apr 15, 2019	    			       —
—														       —
————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————


This README.txt contains the following sections:

	- OVERVIEW

	- STARTUP

	- NOTES

	- STRUCTURE


——————OVERVIEW——————

This project uses the Apache Hadoop framework with the Hadoop Distributed File System (HDFS) to run an analysis on
the Million Song Dataset. Gradle is used for build automation, and can be executing manually with `gradle clean;
gralde build`. The application is constructed within a multi-layer package under `src/main`. Thus, the build
directory will be constructed with the compiled class files under `/build/classes/java/main`, and the Hadoop JAR file
under `/build/libs`.

It is important that a local or shared HDFS and the appropriate configuration files have been set up prior to
running. The configuration for the local HDFS is found under /conf, and the shared 

Once the JAR file has been packaged, and the appropriate resources are started for HDFS and YARN, the application
can be executed by running:

	$HADOOP_HOME/bin/hadoop fs -rm -R ${OUT_DIR}/${CLASS_JOB} ||: \
	&& $HADOOP_HOME/bin/hadoop jar build/libs/${JAR_FILE} cs455.hadoop.${CLASS_JOB}.MainJob \
	$FIRST_INPUT $SECOND_INPUT ${OUT_DIR}/${CLASS_JOB}


——————STARTUP——————

To simplify the process of compiling the application and running the various jobs, the provided run script can be
used. Within this script, various variables can be modified to change the JAR file or HDFS output file. This script
provides the ability to specify whether or not the application should run on the local or shared HDFS.

	1. SSH to namenode ( providence ), and start HDFS and YARN

		$HADOOP_HOME/sbin/start-dfs.sh

	2. Modify the application configurations in the 'run.sh' script

		OUT_DIR="/out"
		JAR_FILE="Jason-Stock-HW3-PC.jar"

	3. Build the project using gradle

		gradle build

	4. Using the terminal, execute the run script to start the execute of the jobs

		Usage: ./run.sh -[ 1 | 2 | 3 ] -c -s

    		-1 : Song and Artist Questions Q1 - Q6, Q8
    		-2 : Aggregate Analysis Q7, Q9
    		-3 : Location Analysis Q10

    		-c : Compile
    		-s : Shared HDFS


——————NOTES——————

1. Output Files

	- Questions 1 - 6 & 8 run in one job. Results can be found in one file in HDFS under `/out/basic/`

	- Questions 7 & 9 are ran in two jobs. Results can be found in one file in HDFS under `/out/aggregate/hotness/`
	  and `/out/aggregate/segment/` respectively.

2. Job Explanation

	- 1-6: these questions are managed in one job with two reducer side hash-maps for the artists and songs.
	  These are generically implemented, such that the top-k results and results can simply be measured. This is
	  done by sorting the values on the target type for the generic variable.


	- 7: average segment data is is created by using Hadoop distributed cache from a previous job to pre-allocate
	  the memory and size for each segment type. Multiple reducers are used for each of the segment types. With
	  an allocated memory size, it is possible for the update the running average for each song as they are
	  processed - significantly reducing space complexity and performance of the application.

	  The running average is computed by reducing or extending a sample segment array to fit the averaged size
	  array. This is done by taking values from proportional strides and updating the associated value in the
	  averaged array. There are two ways this is done, depending on the size of the sample segment array, this
	  includes taking (a) longer segments map equally separated values to each value of the globally averaged
	  segment array, or (b) shorter segments map each value to equally separated values of the globally averaged
	  segment array.


	- 8: the most generic and unique artists looks at the list of similar artists for each artist. This is done
	  because the values are similar artists list is computed already following some algorithm, and could give
	  insight to how artists relate to one another. The google PageRank algorithm is used to then see how these
	  similar artists compare and which are seemingly more important one is over another. Thus, showing to be more
	  generic. A single reducer side map is used, containing the artist ID, artist name, list of similar artists,
	  and an initial PageRank value of one. The PageRank values are computed iteratively following the process
	  below:

		a) join the links and ranks on artist_id. Ranks with outgoing artist_id's not in the rank keys are
		   ignored.

		b) compute the contribution for each outgoing link by assigning the link to associated rank divided by
		   the number of outgoing links. This is similar to reduce by key for each artist over all outgoing link.

		c) repeat steps a) and b) for each artist.

		d) update the ranks based on the contributions for all the artists. Then map the values to update the
		   ranks.

		e) repeat steps a) - d) for N iterations

	  The resulting values show artists with a high PageRank value being most similar to others, and thus, being
	  more generic. Whereas there exists some islands in the graph that show to contain artists not similar to
	  anyone else, and thus, being more unique.


	- 9: this is a precursor to question 7, but focuses on coming up with a new song of higher hotness than those in
	  the dataset. This is done by using multiple linear regression to fit a hyper plane to the sample space. The
	  function g(x_n; w) is parameterized by the vector w; and using ordinary least squares, we can approximate the
	  values of w to fit a model. The gradient of the sum of squared errors with respect to w can be computed for
	  each sample, but generalized to the matrix equation: X.T T = X.T X w , X represents all the samples, T
	  represents all the target hotness values, and w are the weights. We can solve for the weights directly, and
  	  approximates a solution for that minimizes the squared error for all X.

 	  After finding a set of parameters w for X, a new sample x_n can be found by traversing in the direction of the
	  gradient that increases g(x_n; w). Thus, finding sample values that result in a target t_n greater than one.


	- 10: This question was broken into two parts that were not significantly related for reasons discussed further.
	  These approaches are detailed in sections (a) and (b) below.

	  	a) My first approach went to answer the question "Can a new song fall into a specific year based off its
		   feature values? Then used to populate missing values?" To answer this, the songs will have to be
		   classified with high enough accuracy to model the data. Tools used included Apache Hadoop, and Apache 
		   Spark. Data was read from HDFS in Hadoop to format the data to be used by machine learning libraries
		   in Spark. The first job in Hadoop formatted data to LIBSVM format:
		   
		   <label> <index1>:<value1> <index2>:<value2> ...
		   .
		   .
		   .
		
		   Each sample x_n was its own entry for some song.  The <label> represented the year for that song. The 
		   feature values used included: artist_hotness, song_hotness, duration, fade_in, key, loudness, mode, 
		   fade_out, and tempo.

		   Once in the desired format, the Scala project can be built using SBT and submitted to Spark. The Spark
		   job was built to classify the songs from the feature values using a Multilayer perceptron classifier.
		   This is based on a feed-forward artificial neural network. There were 10 inputs fed through a deep 
		   neural network structure with varying hidden unit sizes. The final layer passed through a Softmax
		   function using the multinomial logistic loss to minimize the negative log likelihood. Training was done
		   on a partition of the data for various number of iterations. The accuracy could then be tested for the
		   training set and test set. However, the results diminished with various hyperparmeters. Test accuracy
		   would not get above 15% correctly predicted, and the results were subpar. It is possible the features 
		   are not correlated enough through for specific classes, which leads to reason for a second question.

		b) This question kept into consideration the years that songs were produced, but explored the geographical
		   references for these songs. "Where does the most recently recorded song originate from within the
		   dataset?" To answer this, a visual approach was taken to graphically display the coordinate location of
		   songs, and label them by year. Tools for this included Apache Hadoop, and Python for visuals. 
		   A MapReduce job was done over the dataset analysis and metadata files to organize the data by:

		   longitude  latitude  year
		   .
		   .
		   .
		 
		   Since the goal is to look at the most recently recorded song from a given location, the data could be
		   joined by song_id, and organized for unique longitude and latitude values. However, the years could
		   repeated. The span of years ranged from 1929 to 2010, with geographical coordinates from around the
		   world.

		   Once organized, they could be read into Python. From here, the geographical coordinates could be
		   converted to pixel placements on a two-dimensional map of the world following a Mercator Projection.
		   Points were then assigned to a color map to visually distinguish between years. Once computed, an
		   image of all the geographical locations with an associated color representing the year is displayed.
		   A derivation for this projection, code, and resulting image can be seen in the jupyter notebook
		   /notebook/location-notebook.ipynb


——————STRUCTURE——————

Build files:

	- build.gradle
	
		Used to build Java files and libraries then create an executable JAR file for Apache Hadoop jobs.

	- build.sbt

		Used to build Scala files and libraries for Apache Spark job.

	- run.sh

		Used to run MapReduce jobs for all questions.

	- spark-run.sh

		Used to run Spark job for question 10.


The following Java files live under /src/main/java/...

cs455.hadoop.basic: consists of classes responsible for answering questions 1 - 6 & 8.

	- MainJob.java

		This is the entry point when executing the JAR file.

		Multiple input files are read from HDFS to run this single job.

		@author stock

	- AnalysisMap.java

		This class reads the analysis file write out the intermediary file
		as follows:

		< song_id, loudness fade_duration hotness duration danceability
		energy >

		@author stock

	- MetadataMap.java

		This class reads the analysis file write out the intermediary file
		as follows:

		< song_id, song_title artist_name similar_artists artist_id >

		@author stock

	- MainReducer.java

		Reducer class that takes the output from the mapper and organizes
 		the values accordingly.

		This contains logic needed to organize and sort data from the
		mappers. Questions 1 - 6 are calculated and the PageRank code for
		question 8 can be found here

		@author stock


cs455.hadoop.items: consists of classes responsible basic objects needed for cs455.hadoop.basic.

	- Artist.java

		Class representing information regarding an artist. This implements
		the Item.java interface.

		This includes the songs following:

		a) average loudness
		b) total time spent fading in songs
		c) total number of songs
		d) artist name
 		
		@author stock

	- ArtistData.java

		Public enumerator that implements the Data.java class specific to an
		Artist object. This assists in sorting, adding, validating, and
		getting object members.

		@author stock

	- ArtistRank.java

		Class to hold information for computing PageRank for a given
		artist.
	
		This includes the following:
		
		a) Artists Name
		b) Similar Artists ID's
		c) Rank for this Artist

		@author stock

	- Data.java
	
		Interface to define the functions behind a Song and Artist.
		
		@author stock

	- Item.java 

		Declares methods that are used to be implemented by a Song or Artist.

		@author stock

	- Song.java

		Class representing information regarding a stock. This implements
		the Item.java interface.

		This includes the songs following:

		a) hotness
		b) duration
		c) dance & energy
		d) song name
 		
		@author stock

	- SongData.java

		Public enumerator that implements the Data.java class specific to an
		Song object. This assists in sorting, adding, validating, and
		getting object members.

		@author stock


cs455.hadoop.aggregate: consists of classes responsible for answering questions 7 & 9

	- MainJob.java

		This is the entry point when executing the JAR file.

		Multiple input files are read from HDFS, and chained for two jobs.
		The first job will compute question 7, and use the result as
		distributed cache for question 9.

		@author stock

	- AnalysisMap.java

		This class reads the analysis file write out the intermediary file
		as follows:

		< song_id , hotness duration fade-in key loudness mode fade-out
		tempo time-signature >

		@author stock

	- MetadataMap.java

		This class reads the analysis file write out the intermediary file
		as follows:

		< song_id , artist_terms >

		@author stock

	- MainReducer.java

		Reducer class that takes the output from the mapper and organizes
 		the values accordingly.

		The first write out contains the average segment length for each
		segment. This will be used for the next job. The second write out
		is the weights found minimizes the squared error for the sample
		space. The last write out is the new song with a hotness score
		greater than one.

		@author stock

	- SecondAnalysisMap.java

		This class reads the analysis file write out the intermediary file
		as follows:

		< song_id, start-time pitch timbre max-loudness max-loudness-time
		start-loudness >

		@author stock

	- CustomPartitioner.java

		This class sends the data partitions from SecondAnalysisMap to the
		corresponding reducers. There are six for this job for each type. 

		@author stock

	- SecondReducer.java

		Reducer class that takes the output from the mapper and organizes
 		the values accordingly.

		Each Reducer is associated with a specific segment, and will read
 		the average length from cache.

		@author stock


cs455.hadoop.util: package for application utilities, and reused code.

	- DocumentUtilities.java

		Code that is reused for map and reducer classes. This resolves
		code smells across the project.

		@author stock


cs455.hadoop.location: consists of classes responsible for answering questions 10

	- MainJob.java

		This is the entry point when executing the JAR file.

		Multiple input files are read from HDFS, and chained for two jobs.
		The first job will format values in LIBSVM format for Apache Spark,
		and the second to write out coordinates for visualization.

		@author stock

	- AnalysisMap.java

		This class reads the analysis file write out the intermediary file
		as follows:

		< song_id , hotness duration fade-in key loudness mode fade-out
		tempo time-signature >

		@author stock

	- MetadataMap.java

		This class reads the analysis file write out the intermediary file
		as follows:

		< song_id, artist-hotness longitude latitude >

		@author stock

	- MainReducer.java

		Reducer class that takes the output from the mapper and organizes
 		the values accordingly.

		Format the output data in LIBSVM format for Apache Spark as:

		<label> <index1>:<value1> <index2>:<value2>

		@author stock

	- CoordinateReducer.java

		Second reducer class that takes the output from the mapper and
		organizes the values accordingly.

		Format the data as:
	
		longitude  latitude  year

		@author stock


The following Scala files live under /src/main/scala/...


	- Runner.scala

		Run a MultilayerPerceptronClassifier on the data formatted in
		the Hadoop MapReduce Job prior.

		@author stock

--

THANK YOU!!

Jason D. Stock
