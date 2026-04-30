function y = scale_with_config(x, config)
%SCALE_WITH_CONFIG  Test function: multiply input by config.scale_factor.
%   Used to test struct constant inputs in for_each.
    y = x * config.scale_factor;
end
