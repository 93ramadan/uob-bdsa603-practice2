from mrjob.job import MRJob
import pandas as pd
import datetime

'''
     Define Map-Reduce - Sum Energy Consumption - By Household - By Date
     Params: 
          1. fYS - Filter Start Year (Start of Study Time Frame)
          2. fYE - Filter End Year (End of Study Time Frame)
     Input: UK Power Networks
     Output: (Household ID | Date | Energy Consumption)
'''
class MR_SumEC_ByHousehold_ByDate(MRJob):
     StudyTimeFrame_StartYear = None
     StudyTimeFrame_EndYear = None

     def prepareArgs(self):
          self.StudyTimeFrame_StartYear = int(self.options.fYS)
          self.StudyTimeFrame_EndYear = int(self.options.fYE)

     ''' 
          Configure Additional Arguments for the MR Job
     '''
     def configure_args(self):
          super(MR_SumEC_ByHousehold_ByDate, self).configure_args()
          self.add_passthru_arg('--fYS', help="Specify start year to filter readings")
          self.add_passthru_arg('--fYE', help="Specify end year to filter readings")
          
     ''' 
          Define Mapper Function
     '''
     def mapper(self, key, record):
          # Prepare Validation Arguements
          self.prepareArgs()

          # Split Record into Variables
          (householdID, tariffType, readingDate, energyConsumption, acornCategory, _) = record.split(',')
          
          energyConsumption_AsFloat = float(1)
          try:
               # Convert Reading to DateTime
               readingDate_AsDate = pd.to_datetime(readingDate)
               # Adjust Mid-Night Readings to be for the previous date (last 30-minutes of yesterday)
               if (readingDate_AsDate.time() == datetime.time(0,0)):
                    readingDate_AsDate = readingDate_AsDate - datetime.timedelta(minutes = 1)
          except:
               pass
               #yield (f'InvalidDate_{householdID}'), energyConsumption_AsFloat
          else:
               # Filter out excluded dates (Study Time Frame)
               if (self.StudyTimeFrame_StartYear <= readingDate_AsDate.date().year <= self.StudyTimeFrame_EndYear):
                    try:
                         # Convert energy consumption to Float
                         energyConsumption_AsFloat = float(energyConsumption)
                    except:
                         pass
                         #yield (f'InvalidReading_{householdID}'), energyConsumption_AsFloat
                    else:
                         yield (householdID, readingDate_AsDate.strftime('%Y-%m-%d')), energyConsumption_AsFloat
               else:
                    pass
                    #yield (f'OutofTimeFrame_{householdID}'), energyConsumption_AsFloat

     ''' 
          Define Reducer Function
     '''
     def reducer(self, key, energyReadings):
          yield key, sum(energyReadings)

'''
     Define main to run job when called from command line
'''
if __name__ == "__main__":
     MR_SumEC_ByHousehold_ByDate.run()
     # Below code allows to run in debug mode through Visual Studio Code
     # mr_job = MR_SumEC_ByHousehold_ByDate(args=["C:\\Users\\ARamadan\\Documents\\PyProjects\\Practice2\\dataset\\UK_Power_Networks\\Power-Networks-LCL-June2015(withAcornGps)v2_10.csv", "--fYS", "2012", "--fYE", "2013"])
     # with mr_job.make_runner() as runner:
     #      runner.run()
     #      for key, value in mr_job.parse_output(runner.cat_output()):
     #           print(key)
     #           print(value)
