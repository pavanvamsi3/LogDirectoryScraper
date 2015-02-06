# LogDirectoryScraper
This script goes through the given directory, unzips the gz log files, and inserts them into the database.

PREREQUISITES :

1) Create the "log" database
2) Create the following tables : "logtable" for the logs from the given machines, "machine_log" that stores names of files that have been process along with the machine id and date.
3) Use following commands :

   CREATE TABLE `logtable` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `date_time` datetime DEFAULT NULL,
  `url` varchar(512) DEFAULT NULL,
  `ip` varchar(255) NOT NULL,
  `browserid` varchar(255) NOT NULL,
  `device` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `const` (`date_time`,`ip`)
)

CREATE TABLE `machine_log` (
  `m_id` int(11) NOT NULL AUTO_INCREMENT,
  `machine_id` varchar(255) NOT NULL,
  `file_name` varchar(255) NOT NULL,
  `date_time` datetime DEFAULT NULL,
  PRIMARY KEY (`m_id`)
)​

STEPS to be followed :
1) Make sure your Database is ready
2) specify the path of the machines in "master_folder" field
3) give the number of machines in "no_of_machines" field
4) specify the id of each machine in the list "machine"
5) give the details of the database in database = Database("root", "passwd", "localhost", "log"), where "log" is the name of the database that stores the final log.

Results you will get :
1) All the logs from each of the given machines would be stored in the logtable of log database.
2) The machine_log table of log database will provide you with the detail of which files have been processed and when and also where they were taken from.
3) An additional filelog on each machine will give you the name of the files processed so far and status of the process.
4) A currentlog file for additional details about the process.
