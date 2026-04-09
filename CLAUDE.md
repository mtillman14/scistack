NOTE: You do not have access to Python, it is not installed! Please give me Python commands in copy and paste to Terminal format. Multi-line commands should be given in a form that I can use in a tmp.py file

NOTE 2: Whenever an issue is encountered, add logging and/or tests for it as appropriate.

NOTE 3: As much as possible and as appropriate, solutions to problems should live in the corresponding scistack layer. Only GUI-related issues should live in the GUI layer.

This is a software package to facilitate scientific data processing. You should learn more about it by reading the README.md

There are several submodules in this package. Here are the folders that they are in, and their general purpose within the package:

- scifor: The lowest level iteration orchestrator
- sci-matlab: MATLAB wrapper
- scidb-net (optional)
- sciduck: DuckDB database layer
- scidb: The core user-facing abstractions
- scilineage: Lineage package
- scihist: Processing history package

Each package's folder has a README.md file. When you go to look for implementation details, please start by reading the relevant README.md. If you have sufficient information after that, then please answer the question without exploring additional unnecessary files.

The next place to look for context is the docs/claude folder. This folder contains documentation that was written by you previously, specifically to fill conceptual gaps. If you don't find sufficient information there, then look through the integration tests in each package's tests/ folder. If you have sufficient information after that, then please answer the question without exploring additional unnecessary files. Otherwise, look through the relevant source code.

Finally, after you've collected all relevant information (by reading through the README's and stopping, or by then reading through the docs/claude folder and stopping, or by then reading through the tests and stopping, etc.), please always ask the user if they would like to pause and write a file to docs/claude to fill conceptual gaps that you can look at later to better understand that aspect of the code's function.

Also, every time you draft a plan and present it to me for approval, please also write a .claude/plan-name.md file.

Fially, whenever you encounter a bug and don't know where it comes from, please in your approach consider how we can perform diagnostics, whether with print statements, timing individual steps, etc.

### Code Intelligence

Prefer LSP over Grep/Glob/Read for code navigation:

- `goToDefinition` / `goToImplementation` to jump to source
- `findReferences` to see all usages across the codebase
- `workspaceSymbol` to find where something is defined
- `documentSymbol` to list all symbols in a file
- `hover` for type info without reading the file
- `incomingCalls` / `outgoingCalls` for call hierarchy

Before renaming or changing a function signature, use
`findReferences` to find all call sites first.

Use Grep/Glob only for text/pattern searches (comments,
strings, config values) where LSP doesn't help.

After writing or editing code, check LSP diagnostics before
moving on. Fix any type errors or missing imports immediately.
