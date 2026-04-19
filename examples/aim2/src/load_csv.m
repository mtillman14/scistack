function [time, force_left, force_right] = load_csv(filepath)
    data = readtable(filepath);
    time = data.time;
    force_left = data.force_left;
    force_right = data.force_right;
end
