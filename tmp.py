# Let's test how the registration works                                                                                                                              
                                                                                                                                                                    
from scidb.variable import BaseVariable

# Show initial state
print("Initial _all_subclasses:", BaseVariable._all_subclasses)

# Define a test class
class TestVar1(BaseVariable):
    schema_version = 1

print("After defining TestVar1:", BaseVariable._all_subclasses)

# Define another class with a different name
class TestVar2(BaseVariable):
    schema_version = 1

print("After defining TestVar2:", BaseVariable._all_subclasses)

# Test get_subclass_by_name
print("\nget_subclass_by_name('TestVar1'):", BaseVariable.get_subclass_by_name('TestVar1'))
print("get_subclass_by_name('TestVar2'):", BaseVariable.get_subclass_by_name('TestVar2'))
print("get_subclass_by_name('NonExistent'):", BaseVariable.get_subclass_by_name('NonExistent'))

# Test what happens if we redefine a class with the same name
class TestVar1(BaseVariable):
    schema_version = 2

print("\nAfter redefining TestVar1 with schema_version=2:")
print("_all_subclasses:", BaseVariable._all_subclasses)
print("TestVar1 in _all_subclasses:", BaseVariable._all_subclasses.get('TestVar1'))
print("schema_version of registered TestVar1:", BaseVariable._all_subclasses.get('TestVar1').schema_version)