"""Auto-created by the SciStack GUI -- new Sweep/PathInput/Variable/
Constant declarations created from the GUI are appended here."""
import scidb

test = scidb.Parameter(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, description='')

test_pi = scidb.PathInput('{subject}/{subject}_{session}_CPET.csv', root_folder='examples/vo2max/data', name="test_pi")

class cpet_data_raw(scidb.BaseVariable):
    pass
