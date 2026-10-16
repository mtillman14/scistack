# Current State of the Art

In the 21st century, most scientists must use coding as a tool to accomplish their scientific goals.

Many researchers have no interest in advancing their coding skills beyond the level needed to accomplish their scientific goals, leading many scientific codebases to use the most direct approach to solving their problems.

## Shortcomings of Straightforward Coding

- Portability

- Reusability

- Unstructured file loading and saving

- Queryability

- Scalability

- Time-consuming

- Error-prone

## The Script Approach

/// tab | Python

```python
import numpy as np
import scipy.io

subjects = ["S01", "S02", "S03"]
sessions = ["pre", "post"]

for subject in subjects:
    for session in sessions:
        data = scipy.io.loadmat(f"data/{subject}/{session}/raw.mat")
        filtered = lowpass_filter(data["force"], cutoff_hz=10)
        peak = np.max(filtered)
        np.save(f"results/{subject}_{session}_peak_v2_final.npy", peak)
```
///

/// tab | MATLAB

```matlab
subjects = {'S01', 'S02', 'S03'};
sessions = {'pre', 'post'};

for i = 1:numel(subjects)
    for j = 1:numel(sessions)
        data = load(fullfile('data', subjects{i}, sessions{j}, 'raw.mat'));
        filtered = lowpass_filter(data.force, 10);
        peak = max(filtered);
        save(fullfile('results', [subjects{i} '_' sessions{j} '_peak_v2_final.mat']), 'peak');
    end
end
```
///

While straightforward and certainly serviceable, code designed like this suffers from numerous problems with portability, reproducibility, clarity, and sometimes accuracy, especially at typical real-world script lengths of hundreds or thousands of lines.

## The Function Approach

/// tab | Python

```python
import numpy as np
import scipy.io
from typing import Any

subjects = ["S01", "S02", "S03"]
sessions = ["pre", "post"]
load_data_root_folder = "data"
save_data_root_folder = "results"

def fft_low_pass_filter(signal, cutoff_freq, sampling_rate):
    # Transform signal into the frequency domain
    fft_vals = np.fft.fft(signal)
    frequencies = np.fft.fftfreq(len(signal), d=1/sampling_rate)

    # Zero out frequencies higher than the cutoff (both positive and negative frequencies)
    fft_vals[np.abs(frequencies) > cutoff_freq] = 0

    # Perform inverse FFT to get the filtered signal back in the time domain
    filtered_signal = np.fft.ifft(fft_vals)
    return np.real(filtered_signal)

def load_file(root_folder: str, subject: str, session: str) -> pd.DataFrame:
    file_path = root_folder + "/" + subject + "/" session + "/raw.mat"
    data = scipy.io.loadmat(file_path)
    return data

def save_file(value: Any, file_path: str, subject: str, session: str) -> None:
    file_path = root_folder + "/" + subject + "_" + session + "_peak_v2_final.npy"
    np.save(file_path.format(subject=subject, session=session), value)

if __name__ == "__main__":
    for subject in subjects:
        for session in sessions:
            data = load_file(load_data_root_folder, subject=subject, session=session)
            filtered = fft_low_pass_filter(data["force"], cutoff_hz=10, sampling_rate=1000)
            peak = np.max(filtered)
            save_file(peak, save_data_root_folder, subject=subject, session=session)
```
///

/// tab | MATLAB

```matlab
% load_file.m
function [data] = load_file(folderPath, subject, session)
    filePath = fullfile(folderPath, subject, session);
    data = load(filePath);
end

% save_file.m
function [] = save_file(value, folderPath, subject, session)
    filePath = fullfile(folderPath, [subject '_' session '_peak_v2_final.mat]);
    save(filePath, 'value');
end

% lowpass_filter.m
function [filtered] = lowpass_filter(raw, fs, cutoff, order)
    Wn = cutoff / (fs/2);
    [b, a] = butter(order, Wn, 'low');
    filtered = filtfilt(b, a, raw);
end

% main.m
subjects = {'S01', 'S02', 'S03'};
sessions = {'pre', 'post'};
loadFolderPath = "data";
saveFolderPath = "results";
fs = 1000;
cutoff = 15;
order = 4;

for subject = 1:numel(subjects)
    for session = 1:numel(sessions)
        data = load_file(loadFolderPath, subject, session);
        filtered = lowpass_filter(data.force, 10);
        peak = max(filtered);
        save_file(peak, saveFolderPath, subject, session);
    end
end
```
///

This method improves on the script approach with reusable functions. But portability is still very difficult for anything more than a few lines at a time. Additionally, this script is coupled very tightly to the structure of the dataset. Without a rewrite, this computation will only be used for analyzing specific dataset types that happen to contain subjects and sessions.
