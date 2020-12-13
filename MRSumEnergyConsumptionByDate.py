from mrjob.job import MRJob
import pandas as pd
import sys

'''
     Define Map-Reduce Sum Energy Consumption By Date Class
'''
class MR_SumEnergyConsumption_ByDate(MRJob):
     
     #FILES = ["C:\\Users\\ARamadan\\Documents\\PyProjects\\Practice2\\dataset\\UK_Power_Networks\\Power-Networks-LCL-June2015(withAcornGps)v2_10.csv"]
     ''' 
          Define Mapper Function
     '''
     def mapper(self, key, record):
          # sample 1 - working
          #df_record = record.split(',')
          #yield df_record[0], 1

          # sample 2 - using dataframe for datetime - working but slower then sample 1          
          (LCLid, _, _dateTime, reading, acorn, _) = record.split(',')
          try:
               readingDate = pd.to_datetime(_dateTime)
          except:
               return
          else:
               yield readingDate.strftime('%Y-%m-%d'), 1
          
     ''' 
          Define Reducer Function
     '''
     def reducer(self, householdID, counter):
          yield householdID, sum(counter)

'''
     Define Main Function to expose MR Job
'''
if __name__ == "__main__":
     MR_SumEnergyConsumption_ByDate.run()
     # mr_job = MR_SumEnergyConsumption_ByDate(args=["C:\\Users\\ARamadan\\Documents\\PyProjects\\Practice2\\dataset\\UK_Power_Networks\\Power-Networks-LCL-June2015(withAcornGps)v2_10.csv"])
     # with mr_job.make_runner() as runner:
     #      runner.run()
     #      for key, value in mr_job.parse_output(runner.cat_output()):
     #           print(key)
     #           print(value)
