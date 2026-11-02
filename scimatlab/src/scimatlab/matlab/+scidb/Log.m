classdef Log < scifor.Log
%SCIDB.LOG  The same logger as scifor.Log, under the scidb package name.
%
%   Every method and level constant is inherited from +scifor/Log.m (the one
%   owner), so scidb.Log.set_level('DEBUG') and scifor.Log.set_level('DEBUG')
%   are the same call. SciStack's own MATLAB code calls scifor.Log.
end
