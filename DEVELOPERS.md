# Notes for developers

## System requirements

### just

```sh
# macOS
brew install just

# Linux
# Install from https://github.com/casey/just/releases

# Add completion for your shell. E.g. for bash:
source <(just --completions bash)

# Show all available commands
just #  shortcut for just --list
```


## Local development environment


Set up a local development environment with:
```
just devenv
```

## Tests
Run the tests with:
```
just test <args>
```


## Releasing
To make a new release from the `main` branch:
```
just release
```
This will checkout the latest `main`, update the `version` file,
and create a new release PR for you.
