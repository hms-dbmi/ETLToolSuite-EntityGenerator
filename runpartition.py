import os, json, logging, os, subprocess, datetime, time
from subprocess import *

startTime = time.time()

# set project home variable
projecthome = os.environ.get('ETL_PROJECT_HOME', './') # sets where script is executed if envvar is not set
print 'Project Home is set to ' + projecthome
# set variables
with open(projecthome + '/runpartition.json') as json_data:
    data = json.load(json_data)
    # Infrastructure variables
    resourcesdir = data.get('resourcesdir', projecthome +'/resources/')
    datadir = data.get('datadir', projecthome +'/data/')
    studybucket = data.get('studybucket', '')
    mappingdir = data.get('mappingdir', projecthome +'/mappings/')
    writedir = data.get('writedir', projecthome +'/completed/')
    jobconfig = data.get('jobconfig', projecthome +'/resources/')
    mappingfilename = data.get('mappingfilename', projecthome +'/mapping.csv')
    patientmappingfilename = data.get('patientmappingfilename', projecthome +'/mapping.csv.patient')
    patientconfig = data.get('patientconfig', projecthome +'/resources/patient.config')
    maxjobs = data.get('maxjobs', 10)
    jobmemory = data.get('jobmemory', '2g')
    trialid = data.get('trialid', 'default')

    # Job variables
    syncproject = data.get('syncproject', 'N').upper()
    runcurator = data.get('runcurator', 'Y').upper()
    rundataeval = data.get('rundataeval', 'Y').upper()
    runpartitioning = data.get('runpartitioning', 'Y').upper()
    rungenerator = data.get('rungenerator', 'Y').upper()
    rundataload = data.get('rundataload', 'Y').upper()

    # Logging variables
    loglevel = (getattr(logging, data.get('loglevel', 'INFO').upper(), None))
    logdir = data.get('logdir', '/var/logs/')
    clearlogs = data.get('clearlogs', 'Y').upper()
    archivelogs = data.get('archivelogs', 'N').upper()

    # Data Load variables
    # User and pass need to be setup outside this process or be pulled from vault secrets
    dburl = os.environ.get('DB_HOST','')
    dbuser = os.environ.get('DB_USERNAME','root')
    dbpass = os.environ.get('DB_PASSWORD','')
    dbscriptdir = data.get('dbscriptdir','')

#logging
#function to setup logging
def setup_logger(name, log_file, level=loglevel, formatter=''):
    """Function setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

def logmsgs(logger, stdout, stderr):
    if stdout is not None or stdout != '':
        logger.info(''.join(stdout))
    if stderr is not None or stderr != '':
        logger.info(''.join(stderr))

#create loggers
if clearlogs == 'Y':
    if archivelogs == 'Y':
        ts = datetime.datetime.now().isoformat()
        if os.path.isfile(logdir + 'main.log'):
            os.rename(logdir + 'main.log', logdir + ts +'_main.log')
        if os.path.isfile(logdir + 'errorlogger.log'):
            os.rename(logdir + 'errorlogger.log', logdir + ts +'_errorlogger.log')
        mainlogger = setup_logger('mainlogger',logdir + 'main.log', loglevel, logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        errorlogger = setup_logger('errorlogger',logdir + 'errorlogger.log', loglevel, logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    else:
        if os.path.isfile(logdir + 'main.log'):
            os.remove(logdir + 'main.log')
        if os.path.isfile(logdir + 'errorlogger.log'):
            os.remove(logdir + 'errorlogger.log')
        mainlogger = setup_logger('mainlogger',logdir + 'main.log', loglevel, logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        errorlogger = setup_logger('errorlogger',logdir + 'errorlogger.log', loglevel, logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
else:
    mainlogger = setup_logger('mainlogger',logdir + 'main.log', loglevel, logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    errorlogger = setup_logger('errorlogger',logdir + 'errorlogger.log', loglevel, logging.Formatter('%(asctime)s %(levelname)s %(message)s'))

def cmdWrapper(*args):
    process = subprocess.Popen(list(args), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return stdout,stderr

## Data Sync
#  Syncs a project's bucket
if syncproject == 'Y':
    #args = ['aws', 's3', 'sync', str(studybucket), projecthome, '--exclude','completed/*','--include', 'data/*','--include','mappings/*','--include','resources/*' ]
    args = ['aws', 's3', 'cp', str(studybucket) + '/data/', projecthome + '/data/', '--recursive' ]
    stdout,stderr = cmdWrapper(*args)
    logmsgs(mainlogger, stdout, stderr)
    
    args = ['aws', 's3', 'cp', str(studybucket) + '/mappings/mapping.csv', projecthome + '/mappings/mapping.csv' ]
    stdout,stderr = cmdWrapper(*args)
    logmsgs(mainlogger, stdout, stderr)
    
    args = ['aws', 's3', 'cp', str(studybucket) + '/mappings/mapping.csv.patient', projecthome + '/mappings/mapping.csv.patient' ]
    stdout,stderr = cmdWrapper(*args)
    logmsgs(mainlogger, stdout, stderr)
    
    args = ['aws', 's3', 'cp', str(studybucket) + '/resources/job.config', projecthome + '/resources/job.config' ]
    stdout,stderr = cmdWrapper(*args)
    logmsgs(mainlogger, stdout, stderr)

    args = ['aws', 's3', 'cp', str(studybucket) + '/runpartition.json', projecthome + 'runpartition.json' ]
    stdout,stderr = cmdWrapper(*args)
    logmsgs(mainlogger, stdout, stderr)


## main
# Data Curator
if runcurator == 'Y':
    args = ['java', '-jar', 'DataCurator.jar', '-propertiesfile', jobconfig]
    mainlogger.info('Starting: ' + ' '.join(args))
    stdout,stderr = cmdWrapper(*args)
    logmsgs(mainlogger, stdout, stderr)
    mainlogger.info('Finished: ' + ' '.join(args))
    #mainlogger.info(''.join(stdout))
    #mainlogger.error(''.join(stderr))
## Data Evaluations

if rundataeval == 'Y':
    args = ['java', '-jar', 'DataEvaluation.jar', '-propertiesfile', jobconfig]
    mainlogger.info('Starting: ' + ' '.join(args))
    stdout,stderr = cmdWrapper(*args)
    logmsgs(mainlogger, stdout, stderr)
    mainlogger.info('Finished: ' + ' '.join(args))
    #mainlogger.info(''.join(stdout))
    #mainlogger.error(''.join(stderr))
## Partitioner
if runpartitioning == 'Y':
    args = ['java', '-jar', 'Partitioner.jar', '-propertiesfile', jobconfig]
    mainlogger.info('Starting: ' + ' '.join(args))
    stdout,stderr = cmdWrapper(*args)
    logmsgs(mainlogger, stdout, stderr)
    mainlogger.info('Finished: ' + ' '.join(args))

## Generate Patients
if rungenerator == 'Y':
    args = ['java', '-jar', 'PatientGenerator.jar', '-propertiesfile', jobconfig ]
    mainlogger.info('Starting: ' + ' '.join(args))
    stdout,stderr = cmdWrapper(*args)
    logmsgs(mainlogger, stdout, stderr)
    mainlogger.info('Finished: ' + ' '.join(args))

## Sequence Patients
    args = ['java', '-jar', 'PatientSequencer.jar', '-propertiesfile', jobconfig ]

    mainlogger.info('Starting: ' + ' '.join(args))
    stdout,stderr = cmdWrapper(*args)
    logmsgs(mainlogger, stdout, stderr)
    mainlogger.info('Finished: ' + ' '.join(args))


## Process partitions this will generate the rest of the entities.
    #for file in os.listdir(resourcesdir):
     #   if 'config.part' in file:
            #args = ['java', '-jar', 'EntityGenerator.jar', '-propertiesfile', resourcesdir + file, '-jobtype', 'CSVToI2b2TM' ]
    args = ['sh', 'runpartition.sh', '-j', str(maxjobs), '-m', jobmemory, '-c', 'config.part*.config', '-r', resourcesdir]

    mainlogger.info('Starting: ' + ' '.join(args))
    stdout,stderr = cmdWrapper(*args)
    logmsgs(mainlogger, stdout, stderr)
    mainlogger.info('Finished: ' + ' '.join(args))

## Merge Partitions
    args = ['java', '-jar', 'DataMerge.jar', '-propertiesfile', jobconfig ]

    mainlogger.info('Starting: ' + ' '.join(args))
    stdout,stderr = cmdWrapper(*args)
    logmsgs(mainlogger, stdout, stderr)
    mainlogger.info('Finished: ' + ' '.join(args))

## Process Fill in Tree
    args = ['java', '-jar', 'FillInTree.jar', '-propertiesfile', jobconfig ]

    mainlogger.info('Starting: ' + ' '.join(args))
    stdout,stderr = cmdWrapper(*args)
    logmsgs(mainlogger, stdout, stderr)
    logmsgs(mainlogger, stdout, stderr)
    mainlogger.info('Finished: ' + ' '.join(args))

## Switch to i2b2 path separator
    args = ['java', '-jar', 'FixPaths.jar', '-propertiesfile', jobconfig ]

    mainlogger.info('Starting: ' + ' '.join(args))
    stdout,stderr = cmdWrapper(*args)
    logmsgs(mainlogger, stdout, stderr)

## Upload completed data to completed folder
    args = ['aws', 's3', 'rm',  str(studybucket) + '/completed/', '--recursive' ]
    
    args = ['aws', 's3', 'cp',  projecthome + '/completed/', str(studybucket) + '/completed/', '--recursive' ]
    
    mainlogger.info('Starting: ' + ' '.join(args))
    stdout,stderr = cmdWrapper(*args)
    logmsgs(mainlogger, stdout, stderr)
    mainlogger.info('Finished: ' + ' '.join(args))

    totalRunTime = time.time() - startTime

    mainlogger.info(trialid + ' finished in ' + str(totalRunTime)  + ' seconds')
