"""
@auth: vam
"""
import os
import gzip
import csv
import MySQLdb
import datetime
from django.core.management.base import BaseCommand, CommandError

class Command(BaseCommand):
	global logger
	global filelogger
	logger = "current.log"
	filelogger = "filelog.log"
	def handle(self, *args, **options):
		self.initial_setup()

	def initial_setup(self):
		master_folder = "/home/sh/logbackup/"
		machines = [71, 39, 150]
		no_of_machines = len(machines)
		db_user = "root"
		db_password = "toor"
		db_host = "localhost"
		db_db = "log"

		# Checking the given directory and then unzipping the required files
		for machine in machines:
			logfolder = master_folder + str(machine)
			#fileobj = FileConvertor(logfolder)
			if self.directory_check(logfolder):
				os.chdir(logfolder)
			self.total_files(logfolder)
			self.convert(logfolder)
			self.total_files(logfolder)
			self.connect_and_insert(logfolder, machine, db_user, db_password, db_host, db_db)
			print "Processed Machine %d" % machine
		print "Mission Accomplished"

	def directory_check(self, logfolder):
		#Checking whether it's a directory or not
		if not os.path.isdir(logfolder):
			print "Error: Directory Needed"
			return False
		else:
			return True

	def convert(self, logfolder):
		# Extracting the file and converting it into csv
		log_writefile = open(logger, 'a')

		for f in os.listdir(logfolder):
			if f[-2:] == "gz":
				try:
					filename = f
					readfile = gzip.open(filename, 'rb')
					readfile = gzip.open(filename, 'rb')
					outfile = filename[:-3] + ".csv"
					writefile = open(outfile, 'wb')
					writefile.write(readfile.read())
					readfile.close()
					writefile.close()

					#Logging the information
					log_writefile.write("Unzipped the file %s\n" % filename)
				
				except Exception, e:
					raise e
					log_writefile.write("Error while working on the file %s\n" % filename)
		log_writefile.close()

	def total_files(self, logfolder):
		# Keeps the track of the total_files 
		log_writefile = open(logger, 'a')
		uncompressed_files = 0
		compressed_files = 0

		for f in os.listdir(logfolder):
			if f[-2:] == "gz":
				uncompressed_files += 1
			elif f[-3:] == "csv":
				compressed_files += 1
		
		log_writefile.write("uncompressed_files: %d\n" % uncompressed_files)
		log_writefile.write("compressed_files: %d\n" % compressed_files)
		log_writefile.close()

	def connect_and_insert(self, logfolder, machine, user, password, host, db):
		logfolder = logfolder
		log_writefile = open(logger, 'a')
		log_writefile.write("\nDatabase details:\nUser: %s\nDatabase: %s\n" % (user, db))
		log_writefile.close()
		try:
			mydb = MySQLdb.connect(host, user, password, db)
			log_writefile = open(logger, 'a')
			log_writefile.write("\nConnected to the database\n")
			log_writefile.close()
		except Exception, e:
			raise e
			log_writefile = open(logger, 'a')
			log_writefile.write("\nError connecting to the database\n")
			log_writefile.close()
        try:
	    	file_log = open(filelogger, 'a')
	    	file_log.close()
	    	for filename in os.listdir(logfolder):
	    		logfilename = filename
	    		#Checking whether the current file is already processed or not
	    		cursor_machine = mydb.cursor()
	    		cursor_machine.execute('''select count(*) from machine_log where file_name=%s and machine_id=%s''', (filename, machine))
	    		file_count = cursor_machine.fetchall()
	    		if filename[-3:] == "csv" and file_count[0][0] == 0:
	    			cursor = mydb.cursor()
	    			csv_data = csv.reader(file(filename))
	    			for row in csv_data:
	    				string_date = row[0]
	    				flag = 0
	    				try:
	    					datetime.datetime.strptime(string_date, "%Y-%m-%d %H:%M:%S")
	    				except Exception, e:
	    					flag = 1
	    				if(flag != 1):
	    					cursor.execute('''insert ignore into logtable(date_time,url,ip,browserid,device)values(%s,%s,%s,%s,%s)''', row[0:5])
	    					mydb.commit()
	    					cursor.close()
	    			file_log = open(filelogger, 'a')
	    			file_log.write("%s\n" % filename)
	    			file_log.close()
	    			cursor_new = mydb.cursor()
	    			cursor_new.execute('''insert ignore into machine_log(machine_id, file_name, date_time)values('%s','%s','%s')''' % (machine,filename,datetime.datetime.now()))
	    			mydb.commit()
	    			cursor_new.close()
	    		else:
	    			pass
        except Exception, e:
        	print e
	    	log_writefile = open(logger, 'a')
	    	log_writefile.write("\nError inserting into the database\n")
	    	log_writefile.close()
