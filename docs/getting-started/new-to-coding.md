# New to Coding? Start Here

This page is for you if you have never written code before, or you are setting up a new computer. It walks through everything SciStack needs before you [install it](installation.md): a code editor, Python, a *virtual environment* to keep your project's packages tidy, and a few editor extensions.

It takes about 30 minutes. You only do it once per computer.

<!-- Ground truth: requires-python = ">=3.10" in every package's pyproject.toml;
     CI tests 3.10, 3.12 and 3.13 (.github/workflows). Extension ids from the
     VS Code Marketplace. scistack init's .gitignore (scidb/project.py
     _GITIGNORE) lists .venv/ and venv/ (test_project_init.py guards it). -->

## What you'll set up

| Tool | What it is | Why SciStack needs it |
|---|---|---|
| **VS Code** | A free code editor from Microsoft. | You write your analysis code in it, and the SciStack GUI runs inside it. |
| **Python** | The programming language SciStack is written in. | SciStack runs on Python, even if you write your analysis in MATLAB. |
| **A virtual environment** | A private folder of Python packages for one project. | Keeps SciStack and its dependencies separate from every other project on your computer. |
| **VS Code extensions** | Add-ons for the editor. | Python support, and the SciStack GUI. |
| **Git** (optional, recommended) | Version control: a history of every change to your code. | Lets you undo mistakes and share code. SciStack works without it. |

## A few words you'll see

- **Terminal** (also *command line*, *shell*): a window where you type commands instead of clicking. VS Code has one built in: **View → Terminal**, or press ++ctrl+grave++. On Windows this is usually *PowerShell*; on macOS and Linux it is usually *zsh* or *bash*.
- **Command Palette**: VS Code's search box for every command. Open it with ++ctrl+shift+p++ (Windows/Linux) or ++cmd+shift+p++ (macOS), then start typing a command's name.
- **Package**: a bundle of Python code someone else wrote, installed with `pip`. SciStack is a package. So are `numpy`, `pandas` and `scipy`.
- **Interpreter**: the Python program that runs your code. A computer can have several Python versions installed; you tell VS Code which one to use.
- **Project folder** (also *workspace*): the one folder that holds everything for one analysis. You open this folder in VS Code.

When this page shows a command in a grey box, copy it (the copy button is at the top right of the box), paste it into the terminal, and press ++enter++.

## Step 1: Install VS Code

Download it from [code.visualstudio.com](https://code.visualstudio.com/) and run the installer.

/// tab | Windows
Run the downloaded `.exe`. On the **Select Additional Tasks** screen, tick **Add to PATH** and **Add "Open with Code" action to Windows Explorer**. These let you open a folder in VS Code by right-clicking it.
///

/// tab | macOS
Open the downloaded `.zip` and drag **Visual Studio Code** into your **Applications** folder. Then, so you can start VS Code from the terminal: open VS Code, open the Command Palette (++cmd+shift+p++), and run **Shell Command: Install 'code' command in PATH**.
///

/// tab | Linux
Download the `.deb` (Ubuntu, Debian) or `.rpm` (Fedora, RHEL) package and open it with your software installer, or install from the terminal:

```bash
# Ubuntu / Debian
sudo apt install ./code_*.deb

# Fedora / RHEL
sudo dnf install ./code-*.rpm
```

VS Code is also available as a Snap (`sudo snap install code --classic`).
///

## Step 2: Install Python

SciStack supports **Python 3.10 or newer**, and is tested on 3.10, 3.12 and 3.13. If you are starting fresh, install **Python 3.12**.

/// warning | Using SciStack from MATLAB?
MATLAB supports only certain Python versions, and the list depends on your MATLAB release. Check [MathWorks' compatibility table](https://www.mathworks.com/support/requirements/python-compatibility.html) and install the newest Python version that appears in both that table and SciStack's range (3.10+).
///

First, check whether a suitable Python is already installed. Open a terminal and run:

//// tab | Windows
```powershell
py --list
```

This lists every Python version installed by the official installer. If you see `3.10` or higher, skip to [Step 3](#step-3-install-git-optional). If you get an error, or only older versions, install Python:

1. Go to [python.org/downloads/windows](https://www.python.org/downloads/windows/) and download the **Windows installer (64-bit)** for Python 3.12.
2. Run it. On the first screen, **tick "Add python.exe to PATH"** before clicking **Install Now**. This is the step people most often miss.
3. Close and reopen the terminal, then run `py --list` again to check.

Alternatively, from PowerShell: `winget install Python.Python.3.12`.

/// note | "Python was not found; run without arguments to install from the Microsoft Store"
Windows ships a placeholder `python` command that opens the Microsoft Store. If you see this message, Python isn't installed yet (or isn't on PATH). Install it from python.org as above. You can turn the placeholder off in **Settings → Apps → Advanced app settings → App execution aliases** by switching off the two "python" entries.
///
////

//// tab | macOS
```bash
python3 --version
```

If this prints `Python 3.10` or higher, skip to [Step 3](#step-3-install-git-optional). If it prints an older version, or macOS offers to install the "command line developer tools", install Python:

- **Option A (simplest):** download the **macOS 64-bit universal2 installer** for Python 3.12 from [python.org/downloads/macos](https://www.python.org/downloads/macos/) and run it. When it finishes, double-click **Install Certificates.command** in the folder that opens.
- **Option B (if you use [Homebrew](https://brew.sh/)):** `brew install python@3.12`

Close and reopen the terminal, then run `python3 --version` again.

On macOS, the command is `python3`, not `python`, until you activate a virtual environment (Step 4). Inside one, both work.
////

//// tab | Linux
```bash
python3 --version
```

Most distributions include Python. Ubuntu 22.04 ships 3.10 and Ubuntu 24.04 ships 3.12, both supported. You also need the `venv` module, which Debian and Ubuntu package separately:

```bash
# Ubuntu / Debian
sudo apt update
sudo apt install python3 python3-venv python3-pip

# Fedora / RHEL
sudo dnf install python3 python3-pip
```

If your distribution's Python is older than 3.10, install a newer one from your package manager (for example `sudo apt install python3.12 python3.12-venv` where available) and use `python3.12` in place of `python3` below.
////

## Step 3: Install Git (optional)

Git keeps a history of your code, so you can see what changed and go back to a version that worked. SciStack records which version of each function produced each result, but Git is still the right tool for your code itself.

/// tab | Windows
Download and run the installer from [git-scm.com/download/win](https://git-scm.com/download/win). The default options are fine. Or: `winget install Git.Git`.
///

/// tab | macOS
Run `git --version`. If Git isn't installed, macOS offers to install the command line developer tools, which include it. Click **Install**. (Homebrew: `brew install git`.)
///

/// tab | Linux
`sudo apt install git` (Ubuntu/Debian) or `sudo dnf install git` (Fedora/RHEL).
///

## Step 4: Create a project folder and a virtual environment

A **virtual environment** ("venv") is a folder, usually called `.venv`, inside your project. Packages you install while it is *active* go into that folder instead of into your computer's main Python. Every project gets its own, so upgrading a package for one project can never break another.

1. Make a new, empty folder for your project, for example `projects/my-study` in your home folder. Avoid spaces and special characters in the folder name. **Put it on your own computer's disk, not on a network drive or in a OneDrive/Dropbox folder**: the virtual environment and the database are slow and unreliable over a network. In many organizations `Documents` is on the network; see [Keep the project on a local disk](installation.md#keep-the-project-on-a-local-disk).
2. Open it in VS Code: **File → Open Folder…** and choose the folder. If VS Code asks whether you trust the authors of the files in this folder, click **Yes**.
3. Open the terminal in VS Code (**View → Terminal**). It starts in your project folder.
4. Create the virtual environment:

/// tab | Windows
```powershell
py -3.12 -m venv .venv
```

(Replace `3.12` with the version you installed. `py -m venv .venv` uses the newest one.)
///

/// tab | macOS
```bash
python3 -m venv .venv
```
///

/// tab | Linux
```bash
python3 -m venv .venv
```
///

A `.venv` folder appears in your project. You never need to open it or edit anything inside it.

### Activate it

*Activating* makes `python` and `pip` in this terminal mean the ones inside `.venv`. You need to activate the environment in every new terminal, but VS Code does it for you once you've completed [Step 6](#step-6-tell-vs-code-which-python-to-use).

//// tab | Windows
```powershell
# PowerShell (the VS Code default)
.venv\Scripts\Activate.ps1

# Command Prompt (cmd.exe)
.venv\Scripts\activate.bat

# Git Bash
source .venv/Scripts/activate
```

/// note | "running scripts is disabled on this system"
PowerShell blocks scripts by default. Allow scripts for your own user account (this is a one-time change) and try again:

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```
///
////

//// tab | macOS
```bash
source .venv/bin/activate
```
////

//// tab | Linux
```bash
source .venv/bin/activate
```
////

Once it's active, your terminal prompt starts with `(.venv)`. Check that Python now comes from the project folder:

```bash
python -c "import sys; print(sys.executable)"
```

The printed path should end in `.venv/bin/python` (macOS/Linux) or `.venv\Scripts\python.exe` (Windows).

Finally, update `pip`, Python's package installer:

```bash
python -m pip install --upgrade pip
```

To leave the environment, run `deactivate`. To delete it, close VS Code and delete the `.venv` folder; you can always recreate it.

## Step 5: Install VS Code extensions

Open the **Extensions** view (the four-squares icon in the left bar, or ++ctrl+shift+x++ / ++cmd+shift+x++), search for each extension by name, and click **Install**.

| Extension | Publisher | Needed? | What it adds |
|---|---|---|---|
| **Python** | Microsoft | **Required** | Running and debugging Python, choosing an interpreter, environment activation. Also installs **Pylance** (autocomplete, error highlighting) and **Python Debugger**. |
| **[SciStack Pipeline GUI](https://marketplace.visualstudio.com/items?itemName=scistack.scistack-gui)** | SciStack | For the GUI | The visual pipeline editor, Plot Studio and Stat Studio. Skip it if you will only write code. |
| **MATLAB** | MathWorks | If you use MATLAB | Syntax highlighting, code analysis, and running MATLAB code from VS Code. |
| **Even Better TOML** | tamasfe | Recommended | Highlighting and error checks for `.toml` files, such as SciStack's [project files](project-files.md). |
| **Jupyter** | Microsoft | Optional | Notebooks (`.ipynb`), for interactive exploration. |
| **Rainbow CSV** | mechatroniks | Optional | Colours each column of a `.csv` file, so it's readable. |
| **Data Wrangler** | Microsoft | Optional | A spreadsheet-like viewer for DataFrames and CSV files. |

## Step 6: Tell VS Code which Python to use

VS Code needs to know to use the `.venv` you just made, not some other Python on your computer.

1. Open the Command Palette and run **Python: Select Interpreter**.
2. Choose the entry that mentions `.venv` (it's usually marked *Recommended*). If it isn't listed, choose **Enter interpreter path…** and pick `.venv/bin/python` (macOS/Linux) or `.venv\Scripts\python.exe` (Windows).
3. Close the terminal (the bin icon) and open a new one. It should now show `(.venv)` automatically.

From now on, opening this folder in VS Code uses this environment, and the SciStack GUI runs on it too.

## Step 7: Install SciStack

With the environment active, follow [Installation](installation.md). It's one command:

```bash
pip install scistack
```

Install any other packages your analysis needs the same way, for example `pip install scipy matplotlib`.

## Step 8 (if you use Git): keep the environment out of version control

The `.venv` folder is specific to your computer and can be several hundred megabytes, so it shouldn't be committed. When you create a SciStack project (see [Create New Project](create-new-project.md)), the `.gitignore` file it creates already excludes `.venv/` and `venv/`, along with the database file and the log files (`scistack.log`, `scistack-debug.log`). If you named your environment something else, add that folder name to `.gitignore` on its own line, ending in `/`.

## Checklist

Run these in a new VS Code terminal in your project folder. Each should succeed:

```bash
python --version          # 3.10 or higher
python -c "import sys; print(sys.executable)"   # a path inside .venv
python -c "import scidb; print('SciStack is installed')"
```

## Troubleshooting

/// details | `pip install` worked, but `import scidb` says "No module named scidb"
    type: question

The package was installed into a different Python than the one running your code. Make sure the terminal shows `(.venv)`, and always install with `python -m pip install ...` rather than plain `pip`, so the install goes into whichever `python` is active. In VS Code, check the interpreter shown in the bottom-right corner of the window (when a `.py` file is open) points at `.venv`.
///

/// details | The terminal doesn't show `(.venv)`
    type: question

Activate the environment by hand (Step 4), or re-run **Python: Select Interpreter** (Step 6) and open a new terminal. VS Code only activates the environment in terminals opened *after* the interpreter is selected.
///

/// details | `python` runs an old version, or the wrong one
    type: question

Find which program `python` is: `where python` (Windows) or `which python` (macOS/Linux). If the first result isn't inside `.venv`, the environment isn't active. If your environment was created with the wrong version, delete the `.venv` folder and recreate it with the version you want (`py -3.12 -m venv .venv` on Windows, `python3.12 -m venv .venv` elsewhere).
///

/// details | I moved or renamed my project folder and now nothing works
    type: question

Virtual environments remember the folder they were created in, so they break when moved. Delete `.venv`, recreate it (Step 4), and reinstall your packages (Step 7).
///

## Next steps

- [Installation](installation.md): install SciStack into the environment.
- [What's a Schema?](whats-a-schema.md): the one idea to understand before you start a project.
- [Writing Functions for SciStack](writing-functions.md): how to shape your analysis code.
