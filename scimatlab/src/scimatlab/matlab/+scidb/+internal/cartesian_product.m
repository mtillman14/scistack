function combos = cartesian_product(value_cells)
%CARTESIAN_PRODUCT  Compute Cartesian product of cell arrays.
%
%   Each element of value_cells is a cell array of values.
%   Returns a cell array of cell arrays, one per combination.
    n = numel(value_cells);
    if n == 0
        combos = {{}};
        return;
    end

    sizes = cellfun(@numel, value_cells);
    idx_args = arrayfun(@(s) 1:s, sizes, 'UniformOutput', false);
    grids = cell(1, n);

    % Dimensions are fed to ndgrid REVERSED so the LAST value set varies
    % fastest, i.e. the first is the outermost loop.
    %
    % That is what scifor means: it is sugar for nested for loops, and schema
    % keys are ordered outermost-first (subject, then session, then trial). It
    % is also what Python does -- scifor/foreach.py:569 builds combos with
    % itertools.product, which varies its LAST argument fastest -- so the two
    % implementations must agree or a MATLAB run and a Python run of the same
    % pipeline emit their rows in different orders.
    %
    % ndgrid's first output varies along dim 1, which is fastest under the
    % linear indexing below; handing it the dimensions back-to-front therefore
    % puts the last key on dim 1. Feeding them in order (what this did before
    % 2026-09-14) varies the FIRST key fastest, producing subject-inner
    % ordering -- caught by
    % TestSciforForEachCategorical/test_categorical_two_keys_nested_struct_output.
    rev = n:-1:1;
    [grids{rev}] = ndgrid(idx_args{rev});

    total = prod(sizes);
    combos = cell(1, total);
    for t = 1:total
        combo = cell(1, n);
        for d = 1:n
            combo{d} = value_cells{d}{grids{d}(t)};
        end
        combos{t} = combo;
    end
end
