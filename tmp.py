from scilineage import make_tuple_unpacking_wrapper

def my_plain_function(x):
    return x * 2

wrapped = make_tuple_unpacking_wrapper(my_plain_function)
print(f"__lineage_wrapper__ = {getattr(wrapped, '__lineage_wrapper__', False)}")
# Will print True — this is the bug