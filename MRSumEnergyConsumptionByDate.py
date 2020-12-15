from mrjob.job import MRJob
import pandas as pd
import datetime
import sys
import os.path as path

'''
     Define Map-Reduce Sum Energy Consumption By Date Class
'''
class MR_SumEnergyConsumption_ByDate(MRJob):
     df_Households = None
     df_ACORN = None
     StudyTimeFrame_StartYear = None
     StudyTimeFrame_EndYear = None

     def prepareFilesandArgs(self):
          try:
               if (self.options.fHH is not None):
                    if path.exists(self.options.fHH):
                         df_Households = pd.read_csv(self.options.fHH)
                         df_Households = df_Households.set_index('ID')
          except:
               df_Households = None
          try:
               if (self.options.fAC is not None):
                    if path.exists(self.options.fAC):
                         df_ACORN = pd.read_csv(self.options.fAC)
                         df_ACORN = df_ACORN.set_index('Code')
          except:
               df_ACORN = None
          try:
               if (self.options.fYS is not None):
                    StudyTimeFrame_StartYear = self.options.fYS 
          except:
               StudyTimeFrame_StartYear = None
          try:
               if (self.options.fYE is not None):
                    StudyTimeFrame_EndYear = self.options.fYE 
          except:
               StudyTimeFrame_EndYear = None

     ''' 
          Configure Additional Arguments for the MR Job
     '''
     def configure_args(self):
          super(MR_SumEnergyConsumption_ByDate, self).configure_args()
          self.add_file_arg('--fHH', help = "Specify CSV which contains cleaned up Households")
          self.add_file_arg('--fAC', help = "Specify CSV which contains cleaned up Acorn Categories")
          self.add_passthru_arg('--fYS', help="Specify start year to filter readings")
          self.add_passthru_arg('--fYE', help="Specify end year to filter readings")
          
     ''' 
          Define Mapper Function
     '''
     def mapper(self, key, record):
          # Prepare Validation Files and Variables
          self.prepareFilesandArgs()

          # Split Record into Variables
          (householdID, tariffType, readingDate, energyConsumption, acornCategory, _) = record.split(',')

          # Check if householdID to be considered before doing any further processing
          if (self.df_Households is not None and householdID not in self.df_Households.index):
               yield f'Skipped_HouseholdID{householdID}', 1
          else:
               acornCode = acornCategory.upper().replace('ACORN-','').strip()
               # Check if ACORN category to be considered before doing any further processing
               if (self.df_ACORN is not None and acornCode not in self.df_ACORN.index):
                    yield f'Skipped_ACORN_{acornCode}', 1
               else:
                    try:
                         # Convert Reading to DateTime
                         readingDate_AsDate = pd.to_datetime(readingDate)
                         if (readingDate_AsDate.time() == datetime.time(0,0)):
                              readingDate_AsDate = readingDate_AsDate - datetime.timedelta(minutes = 1)
                         energyConsumption_AsFloat = float(energyConsumption)
                    except:               
                         yield f'Invalid_{householdID}', energyConsumption
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

'''
     Define Main Function to run Program
'''
def main(args):
     MR_SumEnergyConsumption_ByDate.run()

'''
     Define main to run job when called from command line
'''
if __name__ == "__main__":
     main(sys.argv[1:])
     # Below code allows to run in debug mode through Visual Studio Code
     # mr_job = MR_SumEnergyConsumption_ByDate(args=["C:\\Users\\ARamadan\\Documents\\PyProjects\\Practice2\\dataset\\UK_Power_Networks\\Power-Networks-LCL-June2015(withAcornGps)v2_10.csv"])
     # with mr_job.make_runner() as runner:
     #      runner.run()
     #      for key, value in mr_job.parse_output(runner.cat_output()):
     #           print(key)
     #           print(value)
