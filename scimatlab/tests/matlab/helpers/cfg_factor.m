function y = cfg_factor(cfg)
%CFG_FACTOR  Test function: returns cfg.factor (a struct from a glue node),
%   or the value itself when it is numeric, counting calls in CFG_FACTOR_CALLS.
    global CFG_FACTOR_CALLS %#ok<GVMIS>
    if isempty(CFG_FACTOR_CALLS)
        CFG_FACTOR_CALLS = 0;
    end
    CFG_FACTOR_CALLS = CFG_FACTOR_CALLS + 1;
    if isstruct(cfg)
        y = double(cfg.factor);
    else
        y = double(cfg);
    end
end
