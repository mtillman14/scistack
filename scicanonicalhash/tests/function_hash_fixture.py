"""Fixed functions whose hash is pinned in test_function_hash.py.

Do not edit: ``test_function_hash_unchanged_by_move`` asserts a hard-coded
hex captured from these exact bodies, so any change here (beyond comments and
docstrings) breaks the pin by design.
"""


def helper(a):
    return a * 2 + 1


def f(x, y=3):
    total = 0
    for i in range(x):
        total += helper(i) - y
    return total
