from os import chdir,listdir,mkdir,system
from os.path import exists
from fnmatch import fnmatch
from sys import exit,argv
from shutil import move
import pandas
from statistics import stdev,mean
from time import strftime

# Default directory where data files are located inside the path specified by argv[2]
def_dir='Output'
# def_dir='2'

def VerifySources(timestr):
	BlackListedRuns=[]
	chdir(def_dir)

	
	for src_file in listdir('.'):
		# Find all sources files
		if fnmatch(src_file,'*_src'):
			ExpectedSourceNodes=int(src_file.split('_')[0])-1	# Extract the expected number of nodes form file name
			SourceNodes=rawgencount(src_file)					# Calculate the actual nnumber of participating nodes
			if ExpectedSourceNodes!=SourceNodes:
				BlackListedRuns.append(src_file[:-4])
				if not exists('Blackllist'):
					mkdir('Blackllist')
				# The following function works only on *nix systems and should be updated
				system('cp '+ src_file[:-4]+ '* Blackllist/')

	# Store blacklisted node names in a file in the root directory
	chdir('../')
	if len(BlackListedRuns)>0:
		blackllist_file=open('Blackllist-'+timestr+'.txt','w')
		for item in BlackListedRuns:
	  		blackllist_file.write("%s\n" % item)

def DoTheMath(Phase,KeyExchange,HiddenNodeChange,ConcChange):
	chdir(def_dir)

	# Create required data structures
	delay_results_df=pandas.DataFrame()
	loss_results_df=pandas.DataFrame()
	key_results_df=pandas.DataFrame()
	delay_results_dict={}
	loss_results_dict={}
	key_results_dict={}

	avg_delay_dict={}
	avg_loss_dict={}
	avg_key_dict={}
	
	for rx_file in listdir('.'):
		if fnmatch(rx_file,'*R.csv'):
			# Find Rx files and generate the names of Tx and Key files
			tx_file=(rx_file[:-5]+'S.csv')
			key_file=(rx_file[:-5]+'KRx')

			if KeyExchange==True:
				# Find key delivery percentage
				with open(key_file) as f:
					if Phase==1:
						ExpectedKeyExchanges=int(key_file.split('_')[0])-1
					elif Phase==2 and ConcChange==True and HiddenNodeChange==False:
						ExpectedKeyExchanges=30
					elif Phase==2 and ConcChange==False and HiddenNodeChange==True:
						ExpectedKeyExchanges=int(key_file.split('_')[0])
					else:
						print('Error in the first argument..')
						print(Phase)
						print(ConcChange)
						print(HiddenNodeChange)
						exit()

					# ExpectedKeyExchanges has the expected number of keys to be recieved
					# key_rx has the actual keys recieved
					key_rx=int(f.readline().strip())
					SuccKeyExchRate=(ExpectedKeyExchanges - key_rx)/float(ExpectedKeyExchanges)

			config_set_name=(rx_file.split('_')[0]+'_'+rx_file.split('_')[1]) # Has the number of nodes and CBR rate
			
			# Fill data in DataFrame
			rx_data=pandas.read_csv(rx_file, sep=',',header=None,names=['SQN','Rx_TS','Rx_TTL'])
			tx_data=pandas.read_csv(tx_file, sep=',',header=None,names=['SQN','Tx_TS','Tx_TTL'])

			# Get Tx and Rx count, divide over 3 as size will calculate 3 for RateOfCBRumns
			rx_packet_count=rx_data.size/3
			tx_packet_count=tx_data.size/3
			
			# Aggregate in one DataFrame, calculate delay for each packet and sample standard deviation
			aggr_data_tmp=pandas.merge(rx_data,tx_data,on=['SQN'])
			aggr_data_tmp['Delay'] = aggr_data_tmp['Rx_TS']-aggr_data_tmp['Tx_TS']
			sample_std=stdev(aggr_data_tmp['Delay'])

			# Calculate mean + 3 sigma, and create a new DataFrame with filtered data
			sample_mean=mean(aggr_data_tmp['Delay'])
			sample_threshold=sample_mean+(3*sample_std)
			aggr_data_tmp['Is_Delay_Filtered']=aggr_data_tmp['Delay']<sample_threshold
			aggr_data_filtered_tmp=aggr_data_tmp[aggr_data_tmp.Is_Delay_Filtered==True]

			# Calculate and fill mean data for each sample
			sample_mean_filtered=mean(aggr_data_filtered_tmp['Delay'])
			if config_set_name in delay_results_dict:
				delay_results_dict[config_set_name].append(sample_mean_filtered)
			else:
				delay_results_dict[config_set_name]=list()
				delay_results_dict[config_set_name].append(sample_mean_filtered)

			# Calculate and fill loss data for each sample
			sample_loss=(tx_packet_count - rx_packet_count)/float(tx_packet_count)
			if config_set_name in loss_results_dict:
				loss_results_dict[config_set_name].append(sample_loss)
			else:
				loss_results_dict[config_set_name]=list()
				loss_results_dict[config_set_name].append(sample_loss)

			# Calculate and fill key exchange data for each sample
			if KeyExchange==True:
				if config_set_name in key_results_dict:
					key_results_dict[config_set_name].append(float(SuccKeyExchRate))
				else:
					key_results_dict[config_set_name]=list()
					key_results_dict[config_set_name].append(float(SuccKeyExchRate))

	# Store results in a data frame
	delay_results_df=(pandas.DataFrame.from_dict(delay_results_dict, orient='index')).transpose()
	loss_results_df=(pandas.DataFrame.from_dict(loss_results_dict, orient='index')).transpose()
	if KeyExchange==True:
		key_results_df=(pandas.DataFrame.from_dict(key_results_dict, orient='index')).transpose()

	# Calculate averages for each set
	for i in delay_results_df.keys():
		avg_delay_dict[i]=mean(delay_results_dict[i])
		avg_loss_dict[i]=mean(loss_results_dict[i])
		if KeyExchange==True:
			avg_key_dict[i]=mean(key_results_dict[i])
	return delay_results_df,loss_results_df,key_results_df,avg_delay_dict,avg_loss_dict,avg_key_dict

def FormatAndStore(NumOfNodes,RateOfCBR,KeyExchange,timestr,delay_results_df,loss_results_df,key_results_df,avg_delay_dict,avg_loss_dict,avg_key_dict):
	# Create required data structures
	avg_delay_df=pandas.DataFrame(columns=RateOfCBR,index=NumOfNodes)
	avg_loss_df=pandas.DataFrame(columns=RateOfCBR,index=NumOfNodes)
	avg_key_df=pandas.DataFrame(columns=RateOfCBR,index=NumOfNodes)

	# Change avg fromt form dict to a table as DataFrame
	for key in avg_delay_dict.keys():
		part1=str(int(key.split('_')[0]))
		part2=key.split('_')[1]
		avg_delay_df[part2][part1]=avg_delay_dict.get(key)
		avg_loss_df[part2][part1]=avg_loss_dict.get(key)
	if KeyExchange==True:
		for key in avg_key_dict.keys():
			part1=str(int(key.split('_')[0]))
			part2=key.split('_')[1]
			avg_key_df[part2][part1]=avg_key_dict.get(key)
	
	analysis_dir='Analysis-'+timestr
	chdir('../')
	mkdir(analysis_dir)
	chdir(analysis_dir)

	# Update column names from 124 to 1024
	avg_delay_df.rename(columns={'124': '1024'}, inplace=True)
	avg_loss_df.rename(columns={'124': '1024'}, inplace=True)

	#Store all results in CSV files
	delay_results_df.to_csv('Results_Delay.csv')
	loss_results_df.to_csv('Results_Loss.csv')

	avg_delay_df.to_csv('Avg_Delay.csv')
	avg_loss_df.to_csv('Avg_Loss.csv')

	if KeyExchange==True:
		avg_key_df.rename(columns={'124': '1024'}, inplace=True)
		key_results_df.to_csv('Results_Key.csv')
		avg_key_df.to_csv('Avg_Key.csv')

def _make_gen(reader):
    b = reader(1024 * 1024)
    while b:
        yield b
        b = reader(1024*1024)

def rawgencount(filename):
    f = open(filename, 'rb')
    f_gen = _make_gen(f.read)
    return sum( buf.count(b'\n') for buf in f_gen )

def main():
	# ARGC and ARGV verification
	if len(argv)!=3:
		print('Execution error...')
		print('You should write the script name followed by simulation phase followed by root data directory:')
		print('1   -> For phase 1.')
		print('2R  -> For phase 2 with variation data rate and the number of hidden nodes.')
		print('2N  -> For phase 2 with variation in the number of concentrator nodes')
		print('Example...\npython ' + '{}' .format(argv[0]).split('/')[-1] + ' 2R FDIPP01' )
		print('Use . as directory to run the script in the CWD')
		exit()
	else:
		if argv[1]=='1':
			Phase=1
		elif argv[1]=='2R':
			Phase=2
			RateChage=True
			HiddenNodeChange=True
			ConcChange=False
		elif argv[1]=='2N':
			Phase=2
			RateChage=False
			HiddenNodeChange=False
			ConcChange=True
		else:
			print('Argument error...')
			exit()
		
	configFileFound=False
	NumOfNodes=''
	RateOfCBR=''

	root_dir=argv[2]
	chdir(root_dir)
	# root_dir='.'

	for root_files in listdir('.'):
		# Find the configuration file
		if fnmatch(root_files,'RunAll*'):
			if configFileFound==True:
				print('Multiple configuration files were found!')
				print('Exiting...')
				exit()
			configFileFound=True
			# Extract run settings
			for line in open(root_files,'r'):
				if 'set1=' in line:
					LHS=line.find('(')
					RHS=line.find(')')
					NumOfNodes=line[LHS+1:RHS].split(' ')
					# NumOfNodes is the number of nodes
				elif '#set3=' in line:
					KeyExchange=False
				elif 'set3=' in line:
					KeyExchange=True
				elif 'set4=' in line:
					LHS=line.find('(')
					RHS=line.find(')')
					RateOfCBR=line[LHS+1:RHS].split(' ')
					# RateOfCBR is the CBR UDP size
					break
			break
	
	if NumOfNodes=='' or RateOfCBR=='' or configFileFound==False:
		print('Error in extracting run settings..')
		exit()

	# Get current time stamp to be appended to created files and diretories
	timestr = strftime('%Y%m%d-%H%M')
	#####TEMP
	# HiddenNodeChange=False
	# ConcChange=True
	# KeyExchange=False
	# Phase=1
	#####TEMP
	

	if Phase==1 or (Phase==2 and ConcChange==True and HiddenNodeChange==False):
		VerifySources(timestr)
	
	delay_results_df,loss_results_df,key_results_df,avg_delay_dict,avg_loss_dict,avg_key_dict = DoTheMath(Phase,KeyExchange,HiddenNodeChange,ConcChange)
	FormatAndStore(NumOfNodes,RateOfCBR,KeyExchange,timestr,delay_results_df,loss_results_df,key_results_df,avg_delay_dict,avg_loss_dict,avg_key_dict)

main()