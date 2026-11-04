function fig = plot_signal_line(signal, filename) %#ok<INUSD>
%PLOT_SIGNAL_LINE  Endpoint test helper: plot_ prefixed, returns a figure.
%   The framework exports the figure to the resolved PathOutput path and
%   stores/records the path.
    fig = figure('Visible', 'off');
    plot(signal(:));
end
