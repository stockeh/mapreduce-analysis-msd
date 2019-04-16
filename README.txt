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

This project uses the Apache Hadoop framework onto of the Hadoop Distributed File System (HDFS) to run an analysis on
the Million Song Dataset. Gradle is used for build automation, and can be executing manually with `gradle clean;
gralde build`. The application is constructed within a multi-layer package under `src/main`. Thus, the build
directory will be constructed with the compiled class files under `/build/classes/java/main`, and the Hadoop JAR file
under `/build/libs`. It is important that a local or shared HDFS and the appropriate configuration files have been
set up prior to running.

Once the JAR file has been packaged, and the appropriate resources are started for HDFS and YARN, the application
can be executed by running:

	$HADOOP_HOME/bin/hadoop fs -rm -R ${OUT_DIR}/${CLASS_JOB} ||: \
	&& $HADOOP_HOME/bin/hadoop jar build/libs/${JAR_FILE} cs455.hadoop.${CLASS_JOB}.MainJob \
	$FIRST_INPUT $SECOND_INPUT ${OUT_DIR}/${CLASS_JOB}


——————STARTUP——————

To simplify the process of compiling the application and running the various jobs, the provided run script can be
used. Within this script, various variables can be modified to change the JAR file or HDFS output file. This script
provides the ability to specify whether or not the application should run on the local or shared HDFS.

	1. Start HDFS and YARN

		$HADOOP_HOME/sbin/start-dfs.sh

	2. (Optional) modify the application configurations in the 'run.sh' script

		OUT_DIR="/out"

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

	- 1-6: these questions are managed with two reducer side hash-maps for the artists and songs. These are
	  generically implemented, such that the top-k results and results can simply be measured. This is done by
	  sorting the values on the target type for the generic variable.

	- 7: average segment data is is created by using Hadoop distributed cache from a previous job to pre-allocate
	  the memory and size for each segment type. Multiple reducers are used for each of the segment types. With
	  an allocated memory size, it is possible for the update the running average for each song as they are
	  processed - significantly reducing space complexity and performance of the application.

	  The running average is computed by reducing or extending a sample segment array to fit the averaged size
	  array. This is done by taking values from proportional strides and updating the associated value in the
	  averaged array. There are two ways this is done, depending on the size of the sample segment array, this
	  includes taking (a) longer segments map equally separated values to each value of the globally averaged segment
	  array, or (b) shorter segments map each value to equally separated values of the globally averaged segment
	  array.

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

		After finding a set of parameters w for X, a new sample x_n can be found by traversing in the direction that
		increases the g(x_n; w). Thus, finding a value greater than one.

	- 10: 


——————STRUCTURE——————

cs455.scaling.client: consists of classes responsible for starting new clients, sending messages to the server, and
collecting statistics.

	- Client.java

		There are multiple Clients in the system that send and receive data
		to the server.

		A client provides the following functionalities:

		- Connect and maintain an active connection to the server.
 		- Regularly send data packets to the server. The pay loads for these
		  data packets are 8 KB of randomly generated bytes. The rate at
		  which each connection will generate packets is R per-second.
		- The client will track hash codes of the data packets that it has
		  sent to the server. A server will acknowledge every packet that it
		  has received by sending the computed hash code back to the
		  respective client.

		@author stock

	- ClientStatistics.java

		Hold statistics for the client that pertain to the number of sent
		and received messages. This class extends TimerTask, and is executed
		by the client ( specifying timer duration ).

		@author stock

	- SenderThread.java

		The sender thread will run continuously sending messages from the
		respective client to the server.

		@author stock

cs455.scaling.server: consists of classes responsible for the server, thread pool, and server-side statistics.

	- Server.java

		Only one server node in the system to manage incoming connections /
		messages.

		A server node will spawn a new thread pool manager with a set
		number of threads. The following functionalities will be provided,
		and rely on this thread pool:

		- Accept incoming network connections from the clients.
		- Accept incoming traffic from these connections.
		- Groups data from the clients together into batches.
		- Replies to clients by sending back a hash code for each message
		  provided.

		@author stock

	- ServerStatistics.java

	 	Server statistics for managing clients and throughput. This class
		extends TimerTask, and is executed by the server ( specifying
		timer duration ).

		@author stock

	- ThreadPoolManager.java

		A manager for the thread pool that creates the specified number of
		threads, and holds the queue of tasks that are needed to execute.

		@author stock

	- WorkerThread.java

		Working thread that processes objects off of the queue.

		When a new task is available on the queue it will be processed by
		the thread.

		@author stock

cs455.scaling.server.taks: A task interface contains classes for receiving and writing data.

	- Task.java

		Public interface to delegate tasks to available working threads.
		This can include; registering new clients with {@link Register},
		reading data from clients via the {@link Receiver}, and sending
		data back to clients with the {@link Sender}.

		@author stock

	- Register.java

		Task to delegate registration of a client with the server
 		(selector).

		@author stock

	- Receiver.java

		Processes data as received from the clients.

		The server will check a set of SelectionKeys, and upon one being
		readable, a new receiver will be made to manage that data. In turn
		adding the data to a collection of buffered data received from all
		clients in the system.

		@author stock

	- Sender.java

		New tasks, containing a list of data, will be processed and sent
 		back to the clients.

		A sender task contains the data and the respective clients for
		where to respond to data. When a new thread is available in thread
		pool manager, a thread will execute this task.

		@author stock

cs455.scaling.util: Package for application utilities, and reused code.

	- Logger.java

		Class used to print <b>info</b> and <b>error</b> logs to the
		console.

		@author stock

	- TransmissionUtilities.java

		Utilities class that are shared between the client and the server.

		@author stock


--

THANK YOU!!

Jason D. Stock
