from mrjob.job import MRJob
import pandas as pd
import datetime
import sys
import getopt
import argparse

'''
     Define Map-Reduce Sum Energy Consumption By Date Class
'''
class MR_SumEnergyConsumption_ByDate(MRJob):
     def configure_args(self):
          super(MR_SumEnergyConsumption_ByDate, self).configure_args()
          self.add_passthru_arg('--fHH', help="Specify CSV which contains cleaned up Households")
          self.add_passthru_arg('--fAC', help="Specify CSV which contains cleaned up Acorn Categories")
          self.add_passthru_arg('--fYS', help="Specify start year to filter readings")
          self.add_passthru_arg('--fYE', help="Specify end year to filter readings")
     ''' 
          Define Mapper Function
     '''
     def mapper(self, key, record):
          yield self.options.fHH , 1
          yield self.options.fYS , 1
          yield self.options.fYE , 1
          # sample 1 - working
          #df_record = record.split(',')
          #yield df_record[0], 1

          fallback_Date = pd.to_datetime(datetime.datetime.now())
          fallback_Reading = float(0)
          # sample 2 - using dataframe for datetime - working but slower then sample 1          
          (_, _, readingDate, energyConsumption, acornCategory, _) = record.split(',')
          try:
               readingDate_AsDate = pd.to_datetime(readingDate)
               energyConsumption_AsFloat = float(energyConsumption)
          except:
               yield 'Invalid', 0
          else:
               yield readingDate_AsDate.strftime('%Y-%m-%d'), energyConsumption_AsFloat

     ''' 
          Define Combiner Function
     '''
     def combiner(self, readingDate, energyReadings):
          yield readingDate, sum(energyReadings)

     ''' 
          Define Reducer Function
     '''
     def reducer(self, readingDate, energyReadings):
          yield readingDate, sum(energyReadings)

def main(argv):
     '''
          Below are additional arguments that have been created to assist and filter data during the map-reduce job
     '''     
     # filter_HouseholdsCSV = ''
     # filter_YearStart = 0
     # filter_YearEnd = 0

     # parser = argparse.ArgumentParser()
     # parser.add_argument("fHH")
     # args = parser.parse_args()
     # print(args.echo)

     # try:
     #      scriptArguments, _ = getopt.getopt(argv, "fHH:fYS:fYE:", ["fHouseholdsCSV=", "fYearStart=", "fYearEnd="])
     #      for arguementKey, arguementValue in scriptArguments:
     #           if arguementKey in ("-fHH", "--fHouseholdsCSV"):
     #                filter_HouseholdsCSV = arguementValue
     #           if arguementKey in ("-fYS", "--fYearStart"):
     #                filter_YearStart = int(arguementValue)
     #           if arguementKey in ("-fYE", "--fYearEnd"):
     #                filter_YearEnd = int(arguementValue)
     # except getopt.GetoptError:
     #      print('Error loading arguments passed with script.')
     #      sys.exit(2)

     print('reached here')
     MR_SumEnergyConsumption_ByDate.run()

'''
     Define Main Function to expose MR Job
'''
if __name__ == "__main__":
     main(sys.argv[1:])
     # mr_job = MR_SumEnergyConsumption_ByDate(args=["C:\\Users\\ARamadan\\Documents\\PyProjects\\Practice2\\dataset\\UK_Power_Networks\\Power-Networks-LCL-June2015(withAcornGps)v2_10.csv"])
     # with mr_job.make_runner() as runner:
     #      runner.run()
     #      for key, value in mr_job.parse_output(runner.cat_output()):
     #           print(key)
     #           print(value)
