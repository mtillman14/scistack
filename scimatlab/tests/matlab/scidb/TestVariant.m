classdef TestVariant < matlab.unittest.TestCase
%TESTVARIANT  Integration tests for scidb.Variant branch_param pinning in for_each.
%
%   Variant pins a for_each input to a specific branch_param variant. It is an
%   orthogonal, load-time filter, composable with Fixed, column selection, and
%   Merge (per-constituent), and order-agnostic with respect to Fixed.

    properties
        test_dir
    end

    methods (TestClassSetup)
        function addPaths(~)
            this_dir = fileparts(mfilename('fullpath'));
            run(fullfile(this_dir, 'setup_paths.m'));
        end
    end

    methods (TestMethodSetup)
        function setupDatabase(testCase)
            testCase.test_dir = tempname;
            mkdir(testCase.test_dir);
            scidb.configure_database( ...
                fullfile(testCase.test_dir, 'test.duckdb'), ...
                ["subject", "session"]);
        end
    end

    methods (TestMethodTeardown)
        function cleanup(testCase)
            try
                scidb.get_database().close();
            catch
            end
            if isfolder(testCase.test_dir)
                rmdir(testCase.test_dir, 's');
            end
        end
    end

    methods (Access = private)
        function makeTwoVariants(~)
        %MAKETWOVARIANTS  RawSignal=5 → two ProcessedSignal variants (offset 10, 20).
        %   ProcessedSignal branch_param add_offset.offset distinguishes them:
        %       offset=10 → 15, offset=20 → 25.
            RawSignal().save(5, 'subject', 1, 'session', 'A');
            scidb.for_each(@add_offset, ...
                struct('x', RawSignal(), 'offset', 10), ...
                {ProcessedSignal()}, ...
                'subject', 1, 'session', "A");
            scidb.for_each(@add_offset, ...
                struct('x', RawSignal(), 'offset', 20), ...
                {ProcessedSignal()}, ...
                'subject', 1, 'session', "A");
        end
    end

    methods (Test)
        % --- Construction guards ---

        function test_variant_rejects_merge(testCase)
            testCase.verifyError( ...
                @() scidb.Variant(scidb.Merge(RawSignal(), ProcessedSignal()), low_hz=20), ...
                'scidb:Variant');
        end

        function test_variant_requires_branch_params(testCase)
            testCase.verifyError(@() scidb.Variant(ProcessedSignal()), ...
                'scidb:Variant');
        end

        function test_variant_rejects_each_of(testCase)
            testCase.verifyError( ...
                @() scidb.Variant(scifor.EachOf(RawSignal(), ProcessedSignal()), offset=10), ...
                'scidb:Variant');
        end

        function test_variant_nested_stays_nested(testCase)
            % Python's scidb.Variant owns the nested merge (and fn= namespacing
            % per level), so MATLAB keeps what the user wrote.
            v = scidb.Variant(scidb.Variant(ProcessedSignal(), offset=10), gain=2);
            testCase.verifyEqual(v.branch_params.gain, 2);
            testCase.verifyFalse(isfield(v.branch_params, 'offset'));
            testCase.verifyTrue(isa(v.var_type, 'scidb.Variant'));
            testCase.verifyEqual(v.var_type.branch_params.offset, 10);
        end

        function test_variant_nested_conflict_raises_at_run(testCase)
            testCase.makeTwoVariants();
            spec = scidb.Variant(scidb.Variant(ProcessedSignal(), offset=10), offset=20);
            try
                scidb.for_each(@double_values, struct('x', spec), {MergedResult()}, ...
                    'subject', 1, 'session', "A");
                testCase.verifyFail('a conflicting nested Variant must raise');
            catch err
                testCase.verifySubstring(err.message, 'Conflicting branch_param');
            end
        end

        function test_variant_stores_options_as_given(testCase)
            v = scidb.Variant(ProcessedSignal(), fn="add_offset", offset=10, code_version="latest");
            testCase.verifyEqual(string(v.branch_params.fn), "add_offset");
            testCase.verifyEqual(string(v.branch_params.code_version), "latest");
        end

        function test_variant_stores_branch_params(testCase)
            v = scidb.Variant(ProcessedSignal(), offset=10);
            testCase.verifyEqual(v.branch_params.offset, 10);
            testCase.verifyTrue(isa(v.var_type, 'ProcessedSignal'));
        end

        % --- Pinning a plain input ---

        function test_variant_pins_one_variant(testCase)
            testCase.makeTwoVariants();

            % Pin offset=10: ProcessedSignal=15, double_values → 30.
            scidb.for_each(@double_values, ...
                struct('x', scidb.Variant(ProcessedSignal(), offset=10)), ...
                {MergedResult()}, ...
                'subject', 1, 'session', "A");

            r = MergedResult().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(r.data, 30, 'AbsTol', 1e-10);
        end

        function test_variant_pins_other_variant(testCase)
            testCase.makeTwoVariants();

            % Pin offset=20: ProcessedSignal=25, double_values → 50.
            scidb.for_each(@double_values, ...
                struct('x', scidb.Variant(ProcessedSignal(), offset=20)), ...
                {MergedResult()}, ...
                'subject', 1, 'session', "A");

            r = MergedResult().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(r.data, 50, 'AbsTol', 1e-10);
        end

        function test_variant_fn_namespaces_the_pin(testCase)
            % fn= is Python's option, not a branch param: it pins
            % add_offset.offset=10 (ProcessedSignal=15, double_values -> 30).
            testCase.makeTwoVariants();
            scidb.for_each(@double_values, ...
                struct('x', scidb.Variant(ProcessedSignal(), fn="add_offset", offset=10)), ...
                {MergedResult()}, ...
                'subject', 1, 'session', "A");
            r = MergedResult().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(r.data, 30, 'AbsTol', 1e-10);
        end

        % --- Variant inside Merge (per-constituent) ---

        function test_variant_in_merge_pins_one_row(testCase)
            testCase.makeTwoVariants();
            PareticSide().save("p", 'subject', 1, 'session', 'A');

            % Without pinning, the two ProcessedSignal variants would produce
            % two merged rows. Pinning offset=10 leaves exactly one.
            scidb.for_each(@table_dims, ...
                struct('data', scidb.Merge( ...
                    scidb.Variant(ProcessedSignal(), offset=10), ...
                    PareticSide())), ...
                {MergedResult()}, ...
                'subject', 1);

            r = MergedResult().load('subject', 1);
            % 1 row (pinned variant), cols: subject, session, ProcessedSignal,
            % PareticSide = 4.
            testCase.verifyEqual(r.data, [1, 4]');
        end

        % --- Variant + Fixed (order-agnostic) ---

        function test_variant_with_fixed_both_orders(testCase)
            % ProcessedSignal variants at session A and B (baseline vs current).
            RawSignal().save(5, 'subject', 1, 'session', 'A');
            RawSignal().save(7, 'subject', 1, 'session', 'B');
            for off = [10 20]
                scidb.for_each(@add_offset, ...
                    struct('x', RawSignal(), 'offset', off), ...
                    {ProcessedSignal()}, ...
                    'subject', 1, 'session', ["A" "B"]);
            end

            % Both orders pin offset=10 and fix session=A → ProcessedSignal=15,
            % double_values → 30, regardless of the current iterated session.
            % Distinct output types avoid same-location variant ambiguity (the
            % two specs have different __inputs keys).
            spec_a = scidb.Fixed(scidb.Variant(ProcessedSignal(), offset=10), 'session', 'A');
            scidb.for_each(@double_values, struct('x', spec_a), {MergedResult()}, ...
                'subject', 1, 'session', "B");
            r_a = MergedResult().load('subject', 1, 'session', 'B');

            spec_b = scidb.Variant(scidb.Fixed(ProcessedSignal(), 'session', 'A'), offset=10);
            scidb.for_each(@double_values, struct('x', spec_b), {DeltaSignal()}, ...
                'subject', 1, 'session', "B");
            r_b = DeltaSignal().load('subject', 1, 'session', 'B');

            testCase.verifyEqual(r_a.data, 30, 'AbsTol', 1e-10);
            testCase.verifyEqual(r_b.data, 30, 'AbsTol', 1e-10);
        end

        % --- Dry run ---

        function test_variant_dry_run(testCase)
            testCase.makeTwoVariants();
            scidb.for_each(@double_values, ...
                struct('x', scidb.Variant(ProcessedSignal(), offset=10)), ...
                {MergedResult()}, ...
                'subject', 1, 'session', "A", ...
                'dry_run', true);
            % Dry run must not save anything.
            testCase.verifyError(@() MergedResult().load(), 'scidb:NotFoundError');
        end
    end
end
