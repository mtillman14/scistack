# Test the where clause fix - paste into tmp.py                                                                                            

from scidb import configure_database, BaseVariable, for_each                                                                               
import numpy as np                                                                                                                       

class TestData(BaseVariable):
    schema_version = 1

class OutputData(BaseVariable):
    schema_version = 1

db = configure_database('/tmp/test_where.db', ['subject', 'trial'])

# Save data
for subj in [1, 2, 3]:
    TestData.save(np.array([subj]), subject=subj, trial=1)

# Test where clause filtering
def identity(x):
    return x

print("Testing: where='subject <= 2'")
result = for_each(
    identity,
    inputs={"x": TestData},
    outputs=[OutputData],
    subject=[1, 2, 3],
    trial=[1],
    where="subject <= 2",
)

print(f"✓ for_each completed!")
print(f"Result rows: {len(result)}")
print(f"✓ Test passed!" if len(result) == 2 else f"✗ Expected 2 rows, got {len(result)}")

# Check that only subjects 1 and 2 were processed
subjects_processed = sorted(result['subject'].unique())
print(f"Subjects processed: {subjects_processed}")
print(f"✓ Correct subjects!" if subjects_processed == ['1', '2'] else f"✗ Wrong subjects!")