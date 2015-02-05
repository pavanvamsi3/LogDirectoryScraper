import os
import gzip
import csv
import MySQLdb

"""Creating a log file of this script for logging the results"""
global logger

"""Creating a log of files which are processed"""
global filelogger

"""Names of the log files"""

logger = "current.log"
filelogger = "filelog.log"

class FileConvertor:
	"""This Class checks the given directory's validity and then converts the zipped log files into csv data"""

	compressed_files = 0
	uncompressed_files = 0

	def __init__(self, logfolder):
		self.logfolder = logfolder

	def directory_check(self):
		
		if not os.path.isdir(self.logfolder):
			print "Error: Directory Needed"
			return False
		else:
			return True

	def convert(self):

		
		log_writefile = open(logger, 'a')

		for f in os.listdir(self.logfolder):
			
			if f[-2:] == "gz":
				
				try:
					filename = f
					readfile = gzip.open(filename, 'rb')
					outfile = filename[:-3] + ".csv"
					writefile = open(outfile, 'wb')
					writefile.write(readfile.read())
					readfile.close()
					writefile.close()
					
					"""Logging the information to a log file"""

					log_writefile.write("Unzipped the file %s\n" % filename)
				
				except Exception, e:
					raise e
					log_writefile.write("Error while working on the file %s\n" % filename)
		
		log_writefile.close()	
			
	def total_files(self):
			
			log_writefile = open(logger, 'a')

			FileConvertor.uncompressed_files = 0
			FileConvertor.compressed_files = 0
			
			for f in os.listdir(self.logfolder):
				if f[-2:] == "gz":
					FileConvertor.uncompressed_files += 1
				elif f[-3:] == "csv":
					FileConvertor.compressed_files += 1

			log_writefile.write("uncompressed_files: %d\n" % FileConvertor.uncompressed_files)
			log_writefile.write("compressed_files: %d\n" % FileConvertor.compressed_files)
			log_writefile.close()

class Database:

	def __init__(self, user, password, host, db):
		self.user = user
		self.password = password
		self.host = host
		self.db = db
		log_writefile = open(logger,'a')
		log_writefile.write("\nDatabase details:\nUser: %s\nDatabase: %s\n" % (self.user, self.db))
		log_writefile.close()

	def connectDB(self, logfolder):
		try:
			mydb = MySQLdb.connect(self.host, self.user, self.password, self.db)
			log_writefile = open(logger, 'a')
			log_writefile.write("\nConnected to the database\n")
			log_writefile.close()

		except Exception, e:
			raise e
			log_writefile = open(logger, 'a')
			log_writefile.write("\nError connecting to the database\n")
			log_writefile.close()

		try:
			"""Creating a filelog.log file, for the first time""" 
			file_log = open(filelogger, 'a')
			file_log.close()

			for filename in os.listdir(logfolder):
				read_filelog = open(filelogger, "rb")
				lines = read_filelog.readlines()
				read_filelog.close()
				logfilename = filename + "\n"

				#Checking whether the current file is already processed or not

				if filename[-3:] == "csv" and logfilename not in lines:
					#Dealing with the DB
					cursor = mydb.cursor()
					csv_data = csv.reader(file(filename))
					for row in csv_data:
						cursor.execute('''insert ignore into logtable(date_time,url,ip,browserid,device)values(%s,%s,%s,%s,%s)''', row[0:5])
					mydb.commit()
					cursor.close()
					#Done Inserting
					
					#Logging the file names after processing 
					file_log = open(filelogger, 'a')
					file_log.write("%s\n" % filename)
					file_log.close()
				else:
					pass
		except Exception, e:
			raise e
			log_writefile = open(logger, 'a')
			log_writefile.write("\nError inserting into the database\n")
			log_writefile.close()

if __name__ == "__main__":

	"""Log folder"""
	logfolder = "/home/sh/logbackup/71"
	
	"""Checking the given directory and then unzipping the required files"""
	fileobj = FileConvertor(logfolder)
	if fileobj.directory_check():
		os.chdir(logfolder)
	fileobj.total_files()
	fileobj.convert()
	fileobj.total_files()

	database = Database("root", "passwd", "localhost", "log")
	database.connectDB(logfolder)
	print "Mission Accomplished"
